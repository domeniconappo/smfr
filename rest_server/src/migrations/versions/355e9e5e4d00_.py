"""empty message

Revision ID: 355e9e5e4d00
Revises: 2292efe3e3e6
Create Date: 2018-07-18 14:56:12.666559

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '355e9e5e4d00'
down_revision = '2292efe3e3e6'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('aggregation', sa.Column('last_tweetid', sa.String(length=100), nullable=True))
    op.drop_index('bbox_index', table_name='nuts2')
    op.create_index('bbox_index', 'nuts2', ['min_lon', 'min_lat', 'max_lon', 'max_lat'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('bbox_index', table_name='nuts2')
    op.create_index('bbox_index', 'nuts2', ['min_lon', 'min_lat', 'max_lon', 'max_lat'], unique=True)
    op.drop_column('aggregation', 'last_tweetid')
    # ### end Alembic commands ###