"""empty message

Revision ID: 14ee6feb7870
Revises: 95df01dfc439
Create Date: 2018-10-29 17:18:04.092638

"""
import sqlalchemy_utils
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
from sqlalchemy.dialects.mysql import LONGTEXT

revision = '14ee6feb7870'
down_revision = '95df01dfc439'
branch_labels = None
depends_on = None


class LongJSONType(sqlalchemy_utils.types.json.JSONType):
    impl = LONGTEXT


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('product',
                    sa.Column('id', sa.Integer(), nullable=False),
                    sa.Column('collection_ids', sqlalchemy_utils.types.scalar_list.ScalarListType(), nullable=True),
                    sa.Column('highlights', LongJSONType(), nullable=True),
                    sa.Column('created_at', sa.TIMESTAMP(), nullable=True),
                    sa.Column('relevant_tweets', LongJSONType(), nullable=True),
                    sa.Column('aggregated', LongJSONType(), nullable=False),
                    sa.PrimaryKeyConstraint('id'),
                    mysql_charset='utf8mb4',
                    mysql_collate='utf8mb4_general_ci',
                    mysql_engine='InnoDB'
                    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('product')
    # ### end Alembic commands ###
