import re
import tempfile
from collections import OrderedDict

from django.db.models.sql import compiler
from django import VERSION as DjangoVersion


# Pattern to scan a column data type string and split the data type from any
# constraints or other included parts of a column definition. Based upon
# <column_definition> from http://msdn.microsoft.com/en-us/library/ms174979.aspx
_re_data_type_terminator = re.compile(
    r'\s*\b(?:' +
    r'filestream|collate|sparse|not|null|constraint|default|identity|rowguidcol' +
    r'|primary|unique|clustered|nonclustered|with|on|foreign|references|check' +
    ')',
    re.IGNORECASE,
)

class SQLCompiler(compiler.SQLCompiler):
    _re_advanced_group_by =  re.compile(r'GROUP BY(.*%s.*)((ORDER BY)|(LIMIT))?', re.MULTILINE)

    def as_sql(self, with_limits=True, with_col_aliases=False, subquery=False):
        sql, params = super(SQLCompiler, self).as_sql(with_limits, with_col_aliases, subquery)

        if self._re_advanced_group_by.search(sql) is not None:
            print "GROUP BY with parameters found: we need to rewrite the sql"

            # we need to rewrite queries that have parameters inside the GROUP BY clause, such as this one:
            #
            #   sql = """SELECT (first_seen >= %s), COUNT(*) FROM "TABLEAU_ALL" GROUP BY (first_seen >= %s)"""
            #   params = ['2016-05-04', '2016-05-04']
            #
            # to this
            #
            #   sql = """
            #       WITH tmp AS (SELECT %s as p0)
            #           SELECT(first_seen >= p0), COUNT(*)
            #           FROM "TABLEAU_ALL", tmp
            #           GROUP BY(first_seen >= p0)
            #   """
            #   params = ['2016-05-04']
            #

            # first, we generate a sequence of unique parameter names p0, p1... for each individual parameter
            param_names = map(lambda (i, p): 'p%d' % (i,), enumerate(params))
            named_params = OrderedDict()
            for i, (param_name, param_value) in enumerate(zip(param_names, params)):
                # for every parameter, find the first parameter in the list that has the same value
                param_names[i] = next(name for (name,value) in zip(param_names, params) if value==param_value)
                # for these repeated parameters, we will use the first name:
                # if a value appears twice (for instance p3 and p5 have the same value), we will use p3 in both places
                named_params[param_names[i]] = param_value
                # print i, param_name, param_value, param_names[i]

            # param_names has a list of replacements for each of the %s in the original query with the correct parameter name
            # named_params has an ordered dict of parameters with unique values

            # generate a unique temporary name
            tmp_name = '"TMP_%s"' % (next(tempfile._get_candidate_names()),)

            # generate the first part of the sql sentence: a %s placeholder for each named parameter
            with_sql = 'WITH {tmp_name} AS (SELECT {named_params})'.format(
                tmp_name=tmp_name,
                named_params=", ".join(map(lambda name: '%s AS {name}'.format(name=name), named_params.keys()))
            )

            # replace %s placeholders with their corresponding named parameters
            for named_param in param_names:
                sql = sql.replace('%s', tmp_name + "." + named_param, 1)

            # combine the two sql parts into a single statement
            sql = with_sql + ' ' + sql.replace('FROM ', 'FROM {tmp_name}, '.format(tmp_name=tmp_name))

            # new param list contains only one item for each distinct parameter value
            params = named_params.values()

        return sql, params


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    # search for after table/column list
    _re_values_sub = re.compile(r'(?P<prefix>\)|\])(?P<default>\s*|\s*default\s*)values(?P<suffix>\s*|\s+\()?', re.IGNORECASE)
    # ... and insert the OUTPUT clause between it and the values list (or DEFAULT VALUES).
    _values_repl = r'\g<prefix> OUTPUT INSERTED.{col} INTO @sqlserver_ado_return_id\g<default>VALUES\g<suffix>'

    def as_sql(self, *args, **kwargs):
        # Fix for Django ticket #14019
        if not hasattr(self, 'return_id'):
            self.return_id = False

        result = super(SQLInsertCompiler, self).as_sql(*args, **kwargs)
        if isinstance(result, list):
            # Django 1.4 wraps return in list
            return [self._fix_insert(x[0], x[1]) for x in result]
        
        sql, params = result
        return self._fix_insert(sql, params)

    def _fix_insert(self, sql, params):
        """
        Wrap the passed SQL with IDENTITY_INSERT statements and apply
        other necessary fixes.
        """
        meta = self.query.get_meta()
        
        if meta.has_auto_field:
            if hasattr(self.query, 'fields'):
                # django 1.4 replaced columns with fields
                fields = self.query.fields
                auto_field = meta.auto_field
            else:
                # < django 1.4
                fields = self.query.columns
                auto_field = meta.auto_field.db_column or meta.auto_field.column
    
            auto_in_fields = auto_field in fields
    
            quoted_table = self.connection.ops.quote_name(meta.db_table)
            if not fields or (auto_in_fields and len(fields) == 1 and not params):
                # convert format when inserting only the primary key without 
                # specifying a value
                sql = 'INSERT INTO {0} DEFAULT VALUES'.format(
                    quoted_table
                )
                params = []
            elif auto_in_fields:
                # wrap with identity insert
                sql = 'SET IDENTITY_INSERT {table} ON;{sql};SET IDENTITY_INSERT {table} OFF'.format(
                    table=quoted_table,
                    sql=sql,
                )

        # mangle SQL to return ID from insert
        # http://msdn.microsoft.com/en-us/library/ms177564.aspx
        if self.return_id and self.connection.features.can_return_id_from_insert:
            col = self.connection.ops.quote_name(meta.pk.db_column or meta.pk.get_attname())

            # Determine datatype for use with the table variable that will return the inserted ID            
            pk_db_type = _re_data_type_terminator.split(meta.pk.db_type(self.connection))[0]
            
            # NOCOUNT ON to prevent additional trigger/stored proc related resultsets
            sql = 'SET NOCOUNT ON;{declare_table_var};{sql};{select_return_id}'.format(
                sql=sql,
                declare_table_var="DECLARE @sqlserver_ado_return_id table ({col_name} {pk_type})".format(
                    col_name=col,
                    pk_type=pk_db_type,
                ),
                select_return_id="SELECT * FROM @sqlserver_ado_return_id",
            )
            
            output = self._values_repl.format(col=col)
            sql = self._re_values_sub.sub(output, sql)

        return sql, params

class SQLInsertCompiler2(compiler.SQLInsertCompiler, SQLCompiler):

    def as_sql_legacy(self):
        # We don't need quote_name_unless_alias() here, since these are all
        # going to be column names (so we can avoid the extra overhead).
        qn = self.connection.ops.quote_name
        opts = self.query.model._meta
        returns_id = bool(self.return_id and
                          self.connection.features.can_return_id_from_insert)

        result = ['INSERT INTO %s' % qn(opts.db_table)]
        result.append('(%s)' % ', '.join([qn(c) for c in self.query.columns]))

        if returns_id:
            result.append('OUTPUT inserted.%s' % qn(opts.pk.column))

        values = [self.placeholder(*v) for v in self.query.values]
        result.append('VALUES (%s)' % ', '.join(values))

        params = self.query.params
        sql = ' '.join(result)

        meta = self.query.get_meta()
        if meta.has_auto_field:
            # db_column is None if not explicitly specified by model field
            auto_field_column = meta.auto_field.db_column or meta.auto_field.column

            if auto_field_column in self.query.columns:
                quoted_table = self.connection.ops.quote_name(meta.db_table)

                if len(self.query.columns) == 1 and not params:
                    result = ['INSERT INTO %s' % quoted_table]
                    if returns_id:
                        result.append('OUTPUT inserted.%s' % qn(opts.pk.column))
                    result.append('DEFAULT VALUES')
                    sql = ' '.join(result)
                else:
                    sql = "SET IDENTITY_INSERT %s ON;\n%s;\nSET IDENTITY_INSERT %s OFF" % \
                        (quoted_table, sql, quoted_table)

        return sql, params

    def as_sql(self):
        if self.connection._DJANGO_VERSION < 14:
            return self.as_sql_legacy()

        # We don't need quote_name_unless_alias() here, since these are all
        # going to be column names (so we can avoid the extra overhead).
        qn = self.connection.ops.quote_name
        opts = self.query.model._meta
        result = ['INSERT INTO %s' % qn(opts.db_table)]

        has_fields = bool(self.query.fields)
        fields = self.query.fields if has_fields else [opts.pk]
        columns = [f.column for f in fields]

        result.append('(%s)' % ', '.join([qn(c) for c in columns]))

        if has_fields:
            params = values = [
                [
                    f.get_db_prep_save(getattr(obj, f.attname) if self.query.raw else f.pre_save(obj, True), connection=self.connection)
                    for f in fields
                ]
                for obj in self.query.objs
            ]
        else:
            values = [[self.connection.ops.pk_default_value()] for obj in self.query.objs]
            params = [[]]
            fields = [None]

        placeholders = [
            [self.placeholder(field, v) for field, v in zip(fields, val)]
            for val in values
        ]

        if self.return_id and self.connection.features.can_return_id_from_insert:
            params = params[0]
            output = 'OUTPUT inserted.%s' % qn(opts.pk.column)
            result.append(output)
            result.append("VALUES (%s)" % ", ".join(placeholders[0]))
            return [(" ".join(result), tuple(params))]

        items = [
            (" ".join(result + ["VALUES (%s)" % ", ".join(p)]), vals)
            for p, vals in zip(placeholders, params)
        ]

        # This section deals with specifically setting the primary key,
        # or using default values if necessary
        meta = self.query.get_meta()
        if meta.has_auto_field:
            # db_column is None if not explicitly specified by model field
            auto_field_column = meta.auto_field.db_column or meta.auto_field.column
            out = []
            for sql, params in items:
                if auto_field_column in columns:
                    quoted_table = self.connection.ops.quote_name(meta.db_table)
                    # If there are no fields specified in the insert..
                    if not has_fields:
                        sql = "INSERT INTO %s DEFAULT VALUES" % quoted_table
                    else:
                        sql = "SET IDENTITY_INSERT %s ON;\n%s;\nSET IDENTITY_INSERT %s OFF" % \
                            (quoted_table, sql, quoted_table)
                out.append([sql, params])
            items = out
        return items


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass

# django's compiler.SQLDateCompiler was removed in 1.8
if DjangoVersion[0] >= 1 and DjangoVersion[1] >= 8:
    
    import warnings
    
    class DeprecatedMeta(type):
        def __new__(cls, name, bases, attrs):
            # if the metaclass is defined on the current class, it's not
            # a subclass so we don't want to warn.
            if attrs.get('__metaclass__') is not cls:
                msg = ('In the 1.8 release of django, `SQLDateCompiler` was ' +
                    'removed.  This was a parent class of `' + name + 
                    '`, and thus `' + name + '` needs to be changed.')
                raise ImportError(msg)
            return super(DeprecatedMeta, cls).__new__(cls, name, bases, attrs)

    class SQLDateCompiler(object):
        __metaclass__ = DeprecatedMeta

    class SQLDateTimeCompiler(object):
        __metaclass__ = DeprecatedMeta
    
else:
    class SQLDateCompiler(compiler.SQLDateCompiler, SQLCompiler):
        pass

    class SQLDateTimeCompiler(compiler.SQLDateCompiler, SQLCompiler):
        pass
