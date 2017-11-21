import datetime
import os

from sqlalchemy import (
    create_engine,
    literal_column,
    select,
    table,
)
from sqlalchemy.orm.exc import (
    NoResultFound,
    MultipleResultsFound,
)

import classifier

from . import (
    Base,
    DataSource,
    DeliveryMechanism,
    Genre,
    MaterializedWork,
    MaterializedWorkWithGenre,
    Timestamp,
)
from config import Configuration

DEBUG = False


class SessionManager(object):

    # Materialized views need to be created and indexed from SQL
    # commands kept in files. This dictionary maps the views to the
    # SQL files.

    MATERIALIZED_VIEW_WORKS = MaterializedWork.__tablename__
    MATERIALIZED_VIEW_WORKS_WORKGENRES = MaterializedWorkWithGenre.__tablename__
    MATERIALIZED_VIEWS = {
        MATERIALIZED_VIEW_WORKS : 'materialized_view_works.sql',
        MATERIALIZED_VIEW_WORKS_WORKGENRES : 'materialized_view_works_workgenres.sql',
    }

    # A function that calculates recursively equivalent identifiers
    # is also defined in SQL.
    RECURSIVE_EQUIVALENTS_FUNCTION = 'recursive_equivalents.sql'

    engine_for_url = {}

    @classmethod
    def engine(cls, url=None):
        url = url or Configuration.database_url()
        return create_engine(url, echo=DEBUG)

    @classmethod
    def sessionmaker(cls, url=None):
        engine = cls.engine(url)
        return sessionmaker(bind=engine)

    @classmethod
    def initialize(cls, url):
        if url in cls.engine_for_url:
            engine = cls.engine_for_url[url]
            return engine, engine.connect()

        engine = cls.engine(url)
        Base.metadata.create_all(engine)

        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files")

        connection = None
        for view_name, filename in cls.MATERIALIZED_VIEWS.items():
            if engine.has_table(view_name):
                continue
            if not connection:
                connection = engine.connect()
            resource_file = os.path.join(resource_path, filename)
            if not os.path.exists(resource_file):
                raise IOError("Could not load materialized view from %s: file does not exist." % resource_file)
            logging.info(
                "Loading materialized view %s from %s.",
                view_name, resource_file)
            sql = open(resource_file).read()
            connection.execute(sql)                

        if not connection:
            connection = engine.connect()

        # Check if the recursive equivalents function exists already.
        query = select(
            [literal_column('proname')]
        ).select_from(
            table('pg_proc')
        ).where(
            literal_column('proname')=='fn_recursive_equivalents'
        )
        result = connection.execute(query)
        result = list(result)

        # If it doesn't, create it.
        if not result:
            resource_file = os.path.join(resource_path, cls.RECURSIVE_EQUIVALENTS_FUNCTION)
            if not os.path.exists(resource_file):
                raise IOError("Could not load recursive equivalents function from %s: file does not exist." % resource_file)
            sql = open(resource_file).read()
            connection.execute(sql)

        if connection:
            connection.close()

        # Load the materialized views.
        MaterializedWorkWithGenre.prepare(engine)
        MaterializedWork.prepare(engine)

        cls.engine_for_url[url] = engine
        return engine, engine.connect()

    @classmethod
    def refresh_materialized_views(self, _db):
        for view_name in self.MATERIALIZED_VIEWS.keys():
            _db.execute("refresh materialized view %s;" % view_name)
            _db.commit()

    @classmethod
    def session(cls, url, initialize_data=True):
        engine = connection = 0
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            engine, connection = cls.initialize(url)
        session = Session(connection)
        if initialize_data:
            session = cls.initialize_data(session)
        return session

    @classmethod
    def initialize_data(cls, session, set_site_configuration=True):
        # Create initial data sources.
        list(DataSource.well_known_sources(session))

        # Load all existing Genre objects.
        Genre.populate_cache(session)
        
        # Create any genres not in the database.
        for g in classifier.genres.values():
            # TODO: On the very first startup this is rather expensive
            # because the cache is invalidated every time a Genre is
            # created, then populated the next time a Genre is looked
            # up. This wouldn't be a big problem, but this also happens
            # on setup for the unit tests.
            Genre.lookup(session, g, autocreate=True)

        # Make sure that the mechanisms fulfillable by the default
        # client are marked as such.
        for content_type, drm_scheme in DeliveryMechanism.default_client_can_fulfill_lookup:
            mechanism, is_new = DeliveryMechanism.lookup(
                session, content_type, drm_scheme
            )
            mechanism.default_client_can_fulfill = True

        # If there is currently no 'site configuration change'
        # Timestamp in the database, create one.
        timestamp, is_new = get_one_or_create(
            session, Timestamp, collection=None, 
            service=Configuration.SITE_CONFIGURATION_CHANGED,
            create_method_kwargs=dict(timestamp=datetime.datetime.utcnow())
        )
        if is_new:
            site_configuration_has_changed(session)
        session.commit()

        # Return a potentially-new Session object in case
        # it was updated by cls.update_timestamps_table
        return session

def get_one(db, model, on_multiple='error', constraint=None, **kwargs):
    """Gets an object from the database based on its attributes.

    :param constraint: A single clause that can be passed into
        `sqlalchemy.Query.filter` to limit the object that is returned.
    :return: object or None
    """
    constraint = constraint
    if 'constraint' in kwargs:
        constraint = kwargs['constraint']
        del kwargs['constraint']

    q = db.query(model).filter_by(**kwargs)
    if constraint is not None:
        q = q.filter(constraint)

    try:
        return q.one()
    except MultipleResultsFound, e:
        if on_multiple == 'error':
            raise e
        elif on_multiple == 'interchangeable':
            # These records are interchangeable so we can use
            # whichever one we want.
            #
            # This may be a sign of a problem somewhere else. A
            # database-level constraint might be useful.
            q = q.limit(1)
            return q.one()
    except NoResultFound:
        return None

def get_one_or_create(db, model, create_method='',
                      create_method_kwargs=None,
                      **kwargs):
    one = get_one(db, model, **kwargs)
    if one:
        return one, False
    else:
        __transaction = db.begin_nested()
        try:
            # These kwargs are supported by get_one() but not by create().
            get_one_keys = ['on_multiple', 'constraint']
            for key in get_one_keys:
                if key in kwargs:
                    del kwargs[key]
            obj = create(db, model, create_method, create_method_kwargs, **kwargs)
            __transaction.commit()
            return obj
        except IntegrityError, e:
            logging.info(
                "INTEGRITY ERROR on %r %r, %r: %r", model, create_method_kwargs, 
                kwargs, e)
            __transaction.rollback()
            return db.query(model).filter_by(**kwargs).one(), False

def flush(db):
    """Flush the database connection unless it's known to already be flushing."""
    is_flushing = False
    if hasattr(db, '_flushing'):
        # This is a regular database session.
        is_flushing = db._flushing
    elif hasattr(db, 'registry'):
        # This is a flask_scoped_session scoped session.
        is_flushing = db.registry()._flushing
    else:
        logging.error("Unknown database connection type: %r", db)
    if not is_flushing:
        db.flush()

def create(db, model, create_method='',
           create_method_kwargs=None,
           **kwargs):
    kwargs.update(create_method_kwargs or {})
    created = getattr(model, create_method, model)(**kwargs)
    db.add(created)
    flush(db)
    return created, True


# class MaterializedWorkWithGenre(BaseMaterializedWork):
    # __tablename__ = SessionManager.MATERIALIZED_VIEW_WORKS_WORKGENRES
    # __table_args__ = {'autoload' : True}
    # works_id = Column(Integer, primary_key=True)
    # workgenres_id = Column(Integer, primary_key=True),
    # license_pool_id = Column(Integer, ForeignKey('licensepools.id'))
# 
    # license_pool = relationship(
        # LicensePool, 
        # primaryjoin="LicensePool.id==MaterializedWorkWithGenre.license_pool_id",
        # foreign_keys=LicensePool.id, lazy='joined', uselist=False)
# 
# class MaterializedWork(BaseMaterializedWork):
    # __table__ = Table(
        # SessionManager.MATERIALIZED_VIEW_WORKS, 
        # Base.metadata, 
        # Column('works_id', Integer, primary_key=True),
        # Column('license_pool_id', Integer, ForeignKey('licensepools.id')),
        # autoload=True,
    # )
# 
    # license_pool = relationship(
        # LicensePool, 
        # primaryjoin="LicensePool.id==MaterializedWork.license_pool_id",
        # foreign_keys=LicensePool.id, lazy='joined', uselist=False)
# 
    # def __repr__(self):
        # return (u'%s "%s" (%s) %s' % (
            # self.works_id, self.sort_title, self.sort_author, self.language,
            # )).encode("utf8")


