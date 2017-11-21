from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    Table,
)

from sqlalchemy.orm import (
    relationship
)

from sqlalchemy.ext.declarative import DeferredReflection

from . import (
    Base,
    LicensePool,
)



class BaseMaterializedWork(DeferredReflection, Base):
    """A mixin class for materialized views that incorporate Work and Edition."""
    __abstract__ = True
    pass


class MaterializedWorkWithGenre(BaseMaterializedWork):
    __tablename__ = 'mv_works_editions_workgenres_datasources_identifiers'
    # __table_args__ = {'autoload' : True}
    works_id = Column(Integer, primary_key=True)
    workgenres_id = Column(Integer, primary_key=True),
    license_pool_id = Column(Integer, ForeignKey('licensepools.id'))

    license_pool = relationship(
        LicensePool, 
        primaryjoin="LicensePool.id==MaterializedWorkWithGenre.license_pool_id",
        foreign_keys=LicensePool.id, lazy='joined', uselist=False)


class MaterializedWork(BaseMaterializedWork):
    __tablename__ = 'mv_works_editions_datasources_identifiers'
    works_id = Column(Integer, primary_key=True)
    license_pool_id = Column(Integer, ForeignKey('licensepools.id'))

    license_pool = relationship(
        LicensePool, 
        primaryjoin="LicensePool.id==MaterializedWork.license_pool_id",
        foreign_keys=LicensePool.id, lazy='joined', uselist=False)

    def __repr__(self):
        return (u'%s "%s" (%s) %s' % (
            self.works_id, self.sort_title, self.sort_author, self.language,
            )).encode("utf8")
