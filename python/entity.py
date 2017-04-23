import psycopg2
import psycopg2.extras
from datetime import datetime


class DatabaseError(Exception):
    pass


class NotFoundError(Exception):
    pass


class ModifiedError(Exception):
    pass


class Entity(object):
    db = psycopg2.connect(database='db', user='maks')

    # ORM part 1
    __delete_query    = 'DELETE FROM "{table}" WHERE {table}_id=%s'
    __insert_query    = 'INSERT INTO "{table}" ({columns}) VALUES ({placeholders}) RETURNING {table}_id'
    __list_query      = 'SELECT * FROM "{table}"'
    __select_query    = 'SELECT * FROM "{table}" WHERE {table}_id=%s'
    __update_query    = 'UPDATE "{table}" SET {columns} WHERE {table}_id=%s'

    # ORM part 2
    __parent_query    = 'SELECT * FROM "{table}" WHERE {parent}_id=%s'
    __sibling_query   = 'SELECT * FROM "{sibling}" NATURAL JOIN "{join_table}" WHERE {table}_id=%s'
    __update_children = 'UPDATE "{table}" SET {parent}_id=%s WHERE {table}_id IN ({children})'

    def __init__(self, id=None):
        if self.__class__.db is None:
            raise DatabaseError()

        self.__cursor = self.__class__.db.cursor(
            cursor_factory=psycopg2.extras.DictCursor
            )
        self.__id       = id
        self.__fields   = {}
        self.__loaded   = False
        self.__modified = False
        self.__table    = self.__class__.__name__.lower()

    def __getattr__(self, name):
        if self.__modified:
            raise ModifiedError('First, save current changes')

        self.__load()

        if name in self._columns:
            return self._get_column(name)
        elif name in self._parents:
            return self._get_parent(name)
        elif name in self._children:
            return self._get_children(name)
        elif name in self._siblings:
            return self._get_siblings(name)

    def __setattr__(self, name, value):
        if name in self._columns:
            self._set_column(name, value)
            self.__modified = True
        elif name in self._parents:
            self._set_parent(name, value)
            self.__modified = True

        super(Entity, self).__setattr__(name, value)

    def __execute_query(self, query, args):
        try:
            self.__cursor.execute(query, args)
            self.__class__.db.commit()
        except psycopg2.DatabaseError:
            self.__class__.db.rollback()

    def __insert(self):
        keys = ', '.join(self.__fields.keys())
        values = ', '.join(['%s'] * len(self.__fields.values()))

        sql_insert = Entity.__insert_query.format(
                                            table=self.__table,
                                            columns=keys,
                                            placeholders=values,
                                        )

        self.__execute_query(sql_insert, (list(self.__fields.values())))
        self.__id = self.__cursor.fetchone()[0]

    def __load(self):
        if self.__loaded:
            return

        sql_load = Entity.__select_query.format(table=self.__table)

        self.__execute_query(sql_load, (self.__id, ))
        self.__fields = self.__cursor.fetchone()

        if not self.__fields:
            raise NotFoundError()

        self.__loaded = True

    def __update(self):
        values = []
        keys = []

        for x, y in self.__fields.items():
            keys.append('{}=%s'.format(x))
            values.append(y)
        else:
            values.append(self.__id)

        sql_update = Entity.__update_query.format(
                table=self.__table,
                columns=', '.join(keys),
            )

        self.__execute_query(sql_update, values)

    def __get_generator(self, klass, statement):
        self.__execute_query(statement, (self.__id,))

        for inst in self.__cursor.fetchall():
            temp_inst = klass()
            temp_inst.__fields = dict(inst)
            temp_inst.__loaded = True
            temp_inst.__id = '{}_id'.format(temp_inst.__table)

            yield temp_inst

    def _get_children(self, name):
        import models

        child_name = self._children[name]
        child_class = getattr(models, child_name)
        sql_child = self.__parent_query.format(
                                        table=child_name.lower(),
                                        parent=self.__table,
                                    )

        return self.__get_generator(
                                child_class, 
                                sql_child,
                                )

    def _get_column(self, name):
        return self.__fields['{}_{}'.format(self.__table, name)]

    def _get_parent(self, name):
        import models

        parent_id = self.__fields['{}_id'.format(name)]
        parent_class = getattr(models, name.capitalize())

        return parent_class(parent_id)


    def _get_siblings(self, name):
        import models

        sibling_name = self._siblings[name]
        lower_sib_name = sibling_name.lower()
        sibling_class = getattr(models, sibling_name)

        join_table = '__'.join(sorted((lower_sib_name, self.__table)))
        sql_sibiling = self.__sibling_query.format(
                                        sibling=lower_sib_name,
                                        join_table=join_table,
                                        table=self.__table,
                                    )

        return self.__get_generator(
                                    sibling_class,
                                    sql_sibiling,
                                )

    def _set_column(self, name, value):
        self.__fields['{}_{}'.format(self.__table, name)] = value

    def _set_parent(self, name, value):
        if isinstance(value, Entity):
            self.__fields['{}_id'.format(name)] = value.id
        else:
            self.__fields['{}_id'.format(name)] = value

    @classmethod
    def all(cls):
        buff_instance = cls()
        sql_all = buff_instance.__list_query.format(table=buff_instance.__table)

        return buff_instance.__get_generator(cls, sql_all)


    def delete(self):
        if self.__id is None:
            raise NotFoundError()

        delete_query = Entity.__delete_query.format(table=self.__table)

        self.__execute_query(delete_query, (self.__id, ))
        self.__fields.clear()

    @property
    def id(self):
        self.__load()

        return self._get_column('id')

    @property
    def created(self):
        self.__load()

        return datetime.fromtimestamp(self._get_column('created'))

    @property
    def updated(self):
        self.__load()

        return datetime.fromtimestamp(self._get_column('updated'))

    def save(self):
        if not self.__modified:
            return

        if self.__id is None:
            self.__insert()
        else:
            self.__update()

        self.__modified = False
        self.__loaded = False
