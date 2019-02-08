"""empty message

Revision ID: 69695feaabdb
Revises: 1164223db0bf
Create Date: 2019-01-14 14:25:16.698012

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '69695feaabdb'
down_revision = '1164223db0bf'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('aggregation', sa.Column('created_at', sa.TIMESTAMP(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('aggregation', 'created_at')
    # ### end Alembic commands ###
