from flask_sqlalchemy import SQLAlchemy as SQLAlchemyBase

from ..models.base import set_query_property, BaseQuery, SMFRModel


class SQLAlchemy(SQLAlchemyBase):
    """Flask extension that integrates alchy with Flask-SQLAlchemy."""
    def __init__(self, app=None, use_native_unicode=True, session_options=None,
                 metadata=None, query_class=BaseQuery, model_class=SMFRModel):
        self.Model = SMFRModel

        super().__init__(app, use_native_unicode, session_options)

    def make_declarative_base(self, model, metadata=None):
        """Creates or extends the declarative base."""
        if self.Model is None:
            self.Model = super().make_declarative_base(model, metadata)
        else:
            set_query_property(self.Model, self.session)
        return self.Model

    @property
    def metadata(self):
        """Returns the metadata"""
        return self.Model.metadata
