from typing import Optional
from sqlmodel import Field, SQLModel
from sqlalchemy import Column, JSON, UniqueConstraint


class ArtifactModel(SQLModel, table=True):
    __tablename__ = "artifacts"
    id: str = Field(primary_key=True)
    type: Optional[str] = Field(default=None)
    workspace: str = Field(index=True)
    parent_id: Optional[str] = Field(default=None)
    alias: Optional[str] = Field(default=None)
    manifest: Optional[dict] = Field(default=None, sa_column=Column(JSON))
    staging: Optional[list] = Field(default=None, sa_column=Column(JSON))
    download_count: float = Field(default=0.0)
    view_count: float = Field(default=0.0)
    file_count: int = Field(default=0)
    created_at: int = Field()
    created_by: Optional[str] = Field(default=None)
    last_modified: int = Field()
    versions: Optional[list] = Field(default=None, sa_column=Column(JSON))
    config: Optional[dict] = Field(default=None, sa_column=Column(JSON))
    secrets: Optional[dict] = Field(default=None, sa_column=Column(JSON))
    __table_args__ = (
        UniqueConstraint("workspace", "alias", name="_workspace_alias_uc"),
    )
