"""init

Revision ID: 9096f050eb04
Revises: 
Create Date: 2024-11-15 05:58:12.181737

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = "9096f050eb04"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "artifacts",
        sa.Column("id", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("type", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("workspace", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("parent_id", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("alias", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("manifest", sa.JSON(), nullable=True),
        sa.Column("staging", sa.JSON(), nullable=True),
        sa.Column("download_count", sa.Float(), nullable=False),
        sa.Column("view_count", sa.Float(), nullable=False),
        sa.Column("file_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.Integer(), nullable=False),
        sa.Column("created_by", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("last_modified", sa.Integer(), nullable=False),
        sa.Column("versions", sa.JSON(), nullable=True),
        sa.Column("config", sa.JSON(), nullable=True),
        sa.Column("secrets", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(
            ["parent_id"],
            ["artifacts.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("workspace", "alias", name="_workspace_alias_uc"),
    )
    op.create_index(
        op.f("ix_artifacts_workspace"), "artifacts", ["workspace"], unique=False
    )
    op.create_table(
        "event_logs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("event_type", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("workspace", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("user_id", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column("data", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_event_logs_timestamp"), "event_logs", ["timestamp"], unique=False
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_event_logs_timestamp"), table_name="event_logs")
    op.drop_table("event_logs")
    op.drop_index(op.f("ix_artifacts_workspace"), table_name="artifacts")
    op.drop_table("artifacts")
    # ### end Alembic commands ###