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
        self.__fields   = {}
        self.__id       = id
        self.__loaded   = False
        self.__modified = False
        self.__table    = self.__class__.__name__.lower()

    def __getattr__(self, name):
        # check, if instance is modified and throw an exception
        # get corresponding data from database if needed
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    getter with name as an argument
        # throw an exception, if attribute is unrecognized

        if self.__modified:
            raise ModifiedError('First, save current changes')

        self.__load()

        result = self._get_column(name)

        return result


    def __setattr__(self, name, value):
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    setter with name and value as arguments or use default implementation

        if name in self._columns:
            self._set_column(name, value)
            self.__modified = True

        super(Entity, self).__setattr__(name, value)


    def __execute_query(self, query, args):
        # execute an sql statement and handle exceptions together with transactions

        try:
            self.__cursor.execute(query, args)
            self.__class__.db.commit()
        except psycopg2.DatabaseError:
            self.__class__.db.rollback()

    def __insert(self):
        # generate an insert query string from fields keys and values and execute it
        # use prepared statements
        # save an insert id

        keys = ', '.join(self.__fields.keys())
        values = ('%s, ' * len(self.__fields.values())).rstrip(', ')

        temp_dict = {
                    'table' : self.__table,
                    'columns' : keys,
                    'placeholders' : values,
                    }
        sql_insert = Entity.__insert_query.format(**temp_dict)

        self.__execute_query(sql_insert, (list(self.__fields.values())))
        self.__id = self.__cursor.fetchone()[0]

    def __load(self):
        # if current instance is not loaded yet — execute select statement and store it's result as an associative array (fields), where column names used as keys

        if self.__loaded:
            return

        sql_load = Entity.__select_query.format(table=self.__table)

        self.__execute_query(sql_load, (self.__id, ))
        self.__fields = self.__cursor.fetchone()

        if not self.__fields:
            raise NotFoundError()

        self.__loaded = True

    def __update(self):
        # generate an update query string from fields keys and values and execute it
        # use prepared statements

        values = []
        keys = ''

        for x, y in self.__fields.items():
            keys = ('{}, {}=%s').format(keys, x)
            values.append(y)
        else:
            values.append(self.__id)

        temp_dict = {
                    'table' : self.__table,
                    'columns' : keys.lstrip(', '),
                    }
        sql_update = Entity.__update_query.format(**temp_dict)

        self.__execute_query(sql_update, (values))

    def _get_children(self, name):
        # return an array of child entity instances
        # each child instance must have an id and be filled with data
        pass

    def _get_column(self, name):
        return self.__fields['{}_{}'.format(self.__table, name)]

    def _get_parent(self, name):
        # ORM part 2
        # get parent id from fields with <name>_id as a key
        # return an instance of parent entity class with an appropriate id
        pass

    def _get_siblings(self, name):
        # ORM part 2
        # get parent id from fields with <name>_id as a key
        # return an array of sibling entity instances
        # each sibling instance must have an id and be filled with data
        pass

    def _set_column(self, name, value):
        self.__fields['{}_{}'.format(self.__table, name)] = value

    def _set_parent(self, name, value):
        # ORM part 2
        # put new value into fields array with <name>_id as a key
        # value can be a number or an instance of Entity subclass
        pass

    @classmethod
    def all(cls):
        # get ALL rows with ALL columns from corrensponding table
        # for each row create an instance of appropriate class
        # each instance must be filled with column data, a correct id and MUST NOT query a database for own fields any more
        # return an array of istances
        
        buff_instance = cls()

        buff_instance.__execute_query(
                Entity.__list_query.format(table=buff_instance.__table), None
            )

        for x in buff_instance.__cursor.fetchall():
            temp = cls()
            temp.__fields = x
            temp.__id = temp._get_column('id')
            temp.__loaded = True
            yield temp

    def delete(self):
        # execute delete query with appropriate id

        if self.__id is None:
            raise NotFoundError()

        delete_query = Entity.__delete_query.format(table=self.__table)

        self.__execute_query(delete_query, (self.__id, ))
        self.__fields.clear()

    @property
    def id(self):
        # try to guess yourself

        self.__load()

        return self._get_column('id')

    @property
    def created(self):
        # try to guess yourself

        self.__load()

        return datetime.fromtimestamp(self._get_column('created'))

    @property
    def updated(self):
        # try to guess yourself

        self.__load()

        return datetime.fromtimestamp(self._get_column('updated'))

    def save(self):
        # execute either insert or update query, depending on instance id
        
        if not self.__modified:
            return

        if self.__id is None:
            self.__insert()
        else:
            self.__update()

        self.__modified = False
        self.__loaded = False
