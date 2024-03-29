"""empty message

Revision ID: 8163c029df4e
Revises: 9322c908cdee
Create Date: 2018-08-29 10:08:02.164143

"""
from alembic import op
import sqlalchemy as sa

from smfrcore.models.sql.migrations.updates import update_nutstables_8163c029df4e

# revision identifiers, used by Alembic.
revision = '8163c029df4e'
down_revision = '9322c908cdee'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('nuts2', sa.Column('country_code3', sa.String(length=5), nullable=True))
    op.add_column('nuts3', sa.Column('country_code3', sa.String(length=5), nullable=True))
    # ### end Alembic commands ###
    update_nutstables_8163c029df4e()


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('nuts3', 'country_code3')
    op.drop_column('nuts2', 'country_code3')
    # ### end Alembic commands ###
