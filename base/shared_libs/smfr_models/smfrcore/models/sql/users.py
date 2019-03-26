from passlib.apps import custom_app_context as pwd_context
from sqlalchemy import Column, Integer, String
from sqlalchemy_utils import ChoiceType

from .base import SMFRModel


class User(SMFRModel):
    __tablename__ = 'users'
    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_general_ci'}

    ROLES = [
        ('admin', 'Admin'),
        ('user', 'Normal User'),
    ]

    id = Column(Integer, primary_key=True)
    name = Column(String(200))
    email = Column(String(100), index=True, unique=True)
    password_hash = Column(String(128))
    role = Column(ChoiceType(ROLES), nullable=False, default='user')

    @classmethod
    def create(cls, name='', email=None, password=None, role=None):
        if not email or not password:
            raise ValueError('Email and Password are required')
        user = cls(name=name, email=email,
                   password_hash=cls.hash_password(password),
                   role=role)
        user.save()
        return user

    @classmethod
    def hash_password(cls, password):
        return pwd_context.encrypt(password)

    def verify_password(self, password):
        return pwd_context.verify(password, self.password_hash)
