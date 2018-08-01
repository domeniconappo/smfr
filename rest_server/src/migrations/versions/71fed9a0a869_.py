"""empty message

Revision ID: 71fed9a0a869
Revises: e9b27708284b
Create Date: 2018-06-28 15:11:00.430890

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '71fed9a0a869'
down_revision = 'e9b27708284b'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('nuts2', 'mean_pl')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('nuts2', sa.Column('mean_pl', mysql.FLOAT(), nullable=True))
    # ### end Alembic commands ###