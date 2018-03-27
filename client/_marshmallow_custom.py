from swagger_marshmallow_codegen.driver import Driver
from marshmallow import Schema, pre_dump, post_dump
from sqlalchemy_utils import ChoiceType


class BaseSchema(Schema):
    """
    Custom marshmallow Schema to allow ChoiceType values from sqlalchemy_utils in schemas,
    otherwise the __repr__ will be used (and it's a string like 'ChoiceType(label="...", value="...")')
    """
    @pre_dump(pass_many=False)
    def choices_values(self, obj):
        if not hasattr(obj, '__table__'):
            return

        columns = obj.__table__.columns
        for c in columns:
            if isinstance(c.type, ChoiceType):
                val = getattr(obj, c.name)
                if hasattr(val, 'value'):
                    val = val.value
                setattr(obj, c.name, val)

    # @post_dump(pass_many=False)
    # def list_values(self, data):
    #     for k, v in data.items():
    #         if not isinstance(v, list):
    #             continue
    #         v = ', '.join(v)
    #         data[k] = v
    #     return data


class CustomDriver(Driver):
    """
    swagger_marshmallow_codegen Driver to use the custom marshmallow schema
    """
    codegen_factory = Driver.codegen_factory.override(schema_class_path="client._marshmallow_custom:BaseSchema")
