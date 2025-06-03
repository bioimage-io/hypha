#!/usr/bin/env python3
"""
Hypha Database Recovery Script (with additional S3 sources)

This script rebuilds a Hypha SQLite database from S3 backups.
It scans S3 for all workspaces and artifacts, including collection artifacts in alternative S3 buckets.
"""

import os
import json
import asyncio
import logging
import argparse
from collections import defaultdict, deque
from pathlib import Path
from botocore.config import Config
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import SQLModel
from aiobotocore.session import get_session
from utils.artifact_model import ArtifactModel
from utils.constants import (
    ENV_FILE,
    DATABASE_PATH,
    LOGLEVEL,
    S3_BUCKET,
    ADDITIONAL_ENV_FILES,
    ADDITIONAL_ENV_KEYS,
    DATABASE_URI,
    DB_USER_DEFAULT,
    DB_NAME_DEFAULT,
    DB_PORT_DEFAULT,
)

# Global dry run flag
DRY_RUN = False


# --- Load env ---
def load_env_file(filepath: str) -> dict:
    env = {}
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                value = value.strip().strip('"').strip("'")
                env[key] = value
    return env


logger = logging.getLogger("hypha_recovery")
logger.setLevel(getattr(logging, LOGLEVEL))
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)

if not DRY_RUN and os.path.exists(DATABASE_PATH):
    os.remove(DATABASE_PATH)
    logger.info("Removed existing database at %s", DATABASE_PATH)
elif DRY_RUN and os.path.exists(DATABASE_PATH):
    logger.info("[DRY RUN] Would remove existing database at %s", DATABASE_PATH)


def safe_join(directory, *pathnames):
    result = directory
    for pathname in pathnames:
        result = result.rstrip("/") + "/" + pathname.lstrip("/")
    return result


async def create_s3_client(config):
    url = config["endpoint_url"]
    if not url.startswith("http"):
        url = "https://" + url
    return get_session().create_client(
        "s3",
        endpoint_url=url.rstrip("/"),
        aws_access_key_id=config["access_key_id"],
        aws_secret_access_key=config["secret_access_key"],
        region_name=config["region_name"],
        config=Config(connect_timeout=60, read_timeout=300),
    )


async def list_objects_async(s3_client, bucket, prefix=None, delimiter="/"):
    prefix = prefix or ""
    response = await s3_client.list_objects_v2(
        Bucket=bucket, Prefix=prefix, Delimiter=delimiter
    )

    def parse(resp):
        out = []
        for p in resp.get("CommonPrefixes", []):
            out.append(
                {
                    "name": p["Prefix"].rstrip(delimiter).split(delimiter)[-1],
                    "type": "directory",
                }
            )
        for obj in resp.get("Contents", []):
            if not obj["Key"].endswith("/"):
                out.append(
                    {
                        "name": obj["Key"].split("/")[-1],
                        "type": "file",
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"],
                    }
                )
        return out

    items = parse(response)
    while response.get("IsTruncated"):
        token = response["NextContinuationToken"]
        response = await s3_client.list_objects_v2(
            Bucket=bucket, Prefix=prefix, Delimiter=delimiter, ContinuationToken=token
        )
        items += parse(response)
    return items


async def init_db(engine):
    if DRY_RUN:
        logger.info("[DRY RUN] Would initialize database tables")
        return
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


async def scan_artifact_prefix(s3_config, prefix, infer_workspace=False):
    artifacts = {}
    total_dirs = 0
    skipped_dirs = 0
    loaded = 0
    failed = 0

    async with await create_s3_client(s3_config) as s3_client:
        logger.info("Scanning bucket: %s, prefix: %s", s3_config["bucket"], prefix)
        try:
            artifact_dirs = await list_objects_async(
                s3_client, s3_config["bucket"], prefix.rstrip("/") + "/", delimiter="/"
            )
        except Exception as e:
            logger.error("Failed to list prefix %s: %s", prefix, e)
            return artifacts

        total_dirs = len(artifact_dirs)
        logger.info("Found %d artifact directories under %s", total_dirs, prefix)

        for artifact_dir in artifact_dirs:
            if artifact_dir["type"] != "directory":
                skipped_dirs += 1
                continue

            artifact_id = artifact_dir["name"]
            version_prefix = safe_join(prefix.rstrip("/"), artifact_id)

            try:
                version_objects = await list_objects_async(
                    s3_client, s3_config["bucket"], version_prefix + "/"
                )
            except Exception as e:
                logger.warning(
                    "Failed to list versions for artifact %s in prefix %s: %s",
                    artifact_id,
                    version_prefix,
                    e,
                )
                failed += 1
                continue

            json_files = [
                obj
                for obj in version_objects
                if obj["type"] == "file" and obj["name"].endswith(".json")
            ]
            if not json_files:
                logger.debug(
                    "No JSON version files for artifact %s under %s",
                    artifact_id,
                    version_prefix,
                )
                continue

            json_files.sort(key=lambda x: x["last_modified"])
            latest_version_key = safe_join(version_prefix, json_files[-1]["name"])

            try:
                response = await s3_client.get_object(
                    Bucket=s3_config["bucket"], Key=latest_version_key
                )
                content = await response["Body"].read()
                data = json.loads(content)
                data.setdefault("id", artifact_id)
                if infer_workspace:
                    workspace = prefix.split("/")[0]
                    data.setdefault("workspace", workspace)
                else:
                    data.setdefault("workspace", "external")
                artifacts[artifact_id] = data
                loaded += 1
                logger.debug(
                    "Loaded artifact %s from %s", artifact_id, latest_version_key
                )
            except Exception as e:
                logger.warning(
                    "Failed to load artifact %s from %s: %s",
                    artifact_id,
                    latest_version_key,
                    e,
                )
                failed += 1

    logger.info(
        "Finished scanning prefix '%s': %d artifacts loaded, %d skipped, %d failed",
        prefix,
        loaded,
        skipped_dirs,
        failed,
    )
    return artifacts


async def rebuild_database(s3_config, additional_s3_sources):
    logger.info("Starting database recovery")
    engine = create_async_engine(DATABASE_URI, echo=False)
    await init_db(engine)
    session_maker = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    artifacts_by_id = {}

    # Main bucket: scan workspace folders
    async with await create_s3_client(s3_config) as s3_client:
        workspaces = await list_objects_async(s3_client, s3_config["bucket"])
        logger.info("Found %d top-level entries in main S3 bucket", len(workspaces))

        for ws in [
            w for w in workspaces if w["type"] == "directory" and w["name"] != "etc"
        ]:
            ws_name = ws["name"]
            artifacts_prefix = safe_join(ws_name, "artifacts")
            logger.info("Scanning workspace: %s", ws_name)
            artifacts = await scan_artifact_prefix(
                s3_config, artifacts_prefix, infer_workspace=True
            )
            logger.info("Workspace %s: %d artifacts found", ws_name, len(artifacts))
            artifacts_by_id.update(artifacts)

    # Additional buckets: use flat prefix
    for source in additional_s3_sources:
        logger.info(
            "Scanning additional S3 source: %s/%s/%s",
            source["endpoint_url"],
            source["bucket"],
            source["prefix"],
        )
        artifacts = await scan_artifact_prefix(
            source, source["prefix"], infer_workspace=False
        )
        logger.info(
            "Source %s:%s: %d artifacts found",
            source["bucket"],
            source["prefix"],
            len(artifacts),
        )
        artifacts_by_id.update(artifacts)

    logger.info(
        "Total artifacts collected: %d. %s into database...",
        len(artifacts_by_id),
        "Would insert" if DRY_RUN else "Inserting",
    )

    inserted = 0
    skipped = 0
    overwritten = 0
    failed = 0

    if DRY_RUN:
        # In dry run mode, just log what would be done
        for artifact_id, artifact_data in artifacts_by_id.items():
            alias = artifact_data.get("alias")
            ws = artifact_data.get("workspace")

            logger.info(
                "[DRY RUN] Would insert artifact: %s, alias: %s, workspace: %s, file_count: %s, created_at: %s, created_by: %s, last_modified: %s",
                artifact_id,
                alias,
                ws,
                artifact_data.get("file_count"),
                artifact_data.get("created_at"),
                artifact_data.get("created_by"),
                artifact_data.get("last_modified"),
            )
            inserted += 1
    else:
        # Actual database operations
        async with session_maker() as session:
            for artifact_id, artifact_data in artifacts_by_id.items():
                try:
                    async with session.begin():
                        alias = artifact_data.get("alias")
                        ws = artifact_data.get("workspace")
                        if alias:
                            stmt = select(ArtifactModel).where(
                                ArtifactModel.workspace == ws,
                                ArtifactModel.alias == alias,
                            )
                            result = await session.execute(stmt)
                            existing_artifact = result.scalar_one_or_none()

                            if existing_artifact:
                                s3_time = artifact_data.get("last_modified", 0)
                                db_time = existing_artifact.last_modified or 0

                                if s3_time > db_time:
                                    logger.warning(
                                        "Duplicate alias found for workspace='%s', alias='%s' — S3 artifact is NEWER (S3: %s, DB: %s). Overwriting.\n"
                                        "→ S3 created_at: %s, file_count: %s\n"
                                        "→ DB created_at: %s, file_count: %s",
                                        ws,
                                        alias,
                                        s3_time,
                                        db_time,
                                        artifact_data.get("created_at"),
                                        artifact_data.get("file_count"),
                                        existing_artifact.created_at,
                                        existing_artifact.file_count,
                                    )
                                    await session.delete(existing_artifact)
                                    await session.flush()
                                    overwritten += 1
                                else:
                                    logger.warning(
                                        "Duplicate alias found for workspace='%s', alias='%s' — S3 artifact is OLDER (S3: %s, DB: %s). Skipping.\n"
                                        "→ S3 created_at: %s, file_count: %s\n"
                                        "→ DB created_at: %s, file_count: %s",
                                        ws,
                                        alias,
                                        s3_time,
                                        db_time,
                                        artifact_data.get("created_at"),
                                        artifact_data.get("file_count"),
                                        existing_artifact.created_at,
                                        existing_artifact.file_count,
                                    )
                                    skipped += 1
                                    continue

                        artifact = ArtifactModel(**artifact_data)
                        session.add(artifact)
                        inserted += 1
                        logger.info(
                            "Inserted artifact: %s, alias: %s, workspace: %s, file_count: %s, created_at: %s, created_by: %s, last_modified: %s",
                            artifact.id,
                            artifact.alias,
                            artifact.workspace,
                            artifact.file_count,
                            artifact.created_at,
                            artifact.created_by,
                            artifact.last_modified,
                        )
                except Exception as e:
                    logger.error("Insert error for artifact %s: %s", artifact_id, e)
                    failed += 1

    logger.info(
        "Database recovery completed. %s: %d, Overwritten: %d, Skipped: %d, Failed: %d",
        "Would insert" if DRY_RUN else "Inserted",
        inserted,
        overwritten,
        skipped,
        failed,
    )


async def remove_orphan_artifacts(session_maker):
    if DRY_RUN:
        logger.info("[DRY RUN] Would scan for orphan artifacts to remove")
        return

    async with session_maker() as session:
        result = await session.execute(select(ArtifactModel))
        all_artifacts = result.all()
        id_set = {a.id for a, in all_artifacts}

        orphans = [
            a for a, in all_artifacts if a.parent_id and a.parent_id not in id_set
        ]

        print(f"🧹 Found {len(orphans)} orphan artifacts to delete...")
        for orphan in orphans:
            print(
                f" - Removing: {orphan.id} (alias: {orphan.alias}, missing parent: {orphan.parent_id})"
            )
            await session.delete(orphan)

        await session.commit()
        print("✅ Orphan cleanup complete.")


def get_s3_config(env_vars):
    return {
        "endpoint_url": env_vars.get(
            "ENDPOINT_URL", os.environ.get("HYPHA_ENDPOINT_URL")
        ),
        "access_key_id": env_vars.get(
            "ACCESS_KEY_ID", os.environ.get("HYPHA_ACCESS_KEY_ID")
        ),
        "secret_access_key": env_vars.get(
            "SECRET_ACCESS_KEY", os.environ.get("HYPHA_SECRET_ACCESS_KEY")
        ),
        "region_name": "us-east-1",
        "bucket": S3_BUCKET,
        "prefix": "artifacts",
    }


def get_additional_s3_sources(additional_env_files, additional_env_keys):
    additional_s3_sources = []
    for env_file in additional_env_files:
        if not Path(env_file).exists():
            logger.warning("Additional env file not found: %s", env_file)
            continue
        env = load_env_file(env_file)
        conf = {k: env.get(k, "") for k in additional_env_keys}
        if not conf["S3_BUCKET"] or not conf["S3_PREFIX"]:
            logger.warning("Skipping incomplete S3 config in %s", env_file)
            continue
        additional_s3_sources.append(
            {
                "endpoint_url": conf["S3_ENDPOINT_URL"],
                "access_key_id": conf["S3_ACCESS_KEY_ID"],
                "secret_access_key": conf["S3_SECRET_ACCESS_KEY"],
                "region_name": conf.get("S3_REGION_NAME", "us-east-1"),
                "bucket": conf["S3_BUCKET"],
                "prefix": conf["S3_PREFIX"],
                "secrets": conf,
            }
        )
    return additional_s3_sources


async def check_pg_table_rows(pg_engine):
    async with pg_engine.begin() as conn:
        result = await conn.execute(text("SELECT COUNT(*) FROM artifacts"))
        count = result.scalar()
        print(f"📊 PostgreSQL: artifacts table has {count} rows.")


def clean_artifact(a: ArtifactModel) -> ArtifactModel:
    """Return a clean ArtifactModel instance without relationships."""
    return ArtifactModel(
        id=a.id,
        type=a.type,
        workspace=a.workspace,
        parent_id=a.parent_id,
        alias=a.alias,
        manifest=a.manifest,
        staging=a.staging,
        download_count=a.download_count,
        view_count=a.view_count,
        file_count=a.file_count,
        created_at=a.created_at,
        created_by=a.created_by,
        last_modified=a.last_modified,
        versions=a.versions,
        config=a.config,
        secrets=a.secrets,
    )


def build_dependency_graph(artifacts):
    graph = defaultdict(list)
    indegree = defaultdict(int)
    id_map = {a.id: a for a in artifacts}

    for a in artifacts:
        if a.parent_id and a.parent_id in id_map:
            graph[a.parent_id].append(a.id)
            indegree[a.id] += 1
        else:
            indegree[a.id] = indegree.get(a.id, 0)

    return graph, indegree, id_map


def topological_sort_artifacts(artifacts):
    graph, indegree, id_map = build_dependency_graph(artifacts)
    queue = deque([aid for aid, deg in indegree.items() if deg == 0])
    ordered = []

    while queue:
        aid = queue.popleft()
        ordered.append(id_map[aid])
        for neighbor in graph[aid]:
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                queue.append(neighbor)

    return ordered


async def transfer_artifacts_topo(pg_engine, sqlite_session_maker, pg_session_maker):
    if DRY_RUN:
        print("🔄 [DRY RUN] Would transfer artifacts with topological ordering")
        return

    async with sqlite_session_maker() as sqlite_session:
        result = await sqlite_session.exec(select(ArtifactModel))
        all_artifacts = result.all()

    print(f"🔄 Transferring with topological order: {len(all_artifacts)} artifacts")

    ordered = topological_sort_artifacts(all_artifacts)
    inserted = 0
    print(ordered)

    async with pg_session_maker() as pg_session:
        async with pg_session.begin():
            for a in ordered:
                try:
                    obj = clean_artifact(a)
                    pg_session.add(obj)
                    inserted += 1
                except Exception as e:
                    print(f"⚠️ Failed to insert {a.id} ({a.alias}): {e}")
        await pg_session.commit()

    print(f"✅ Inserted {inserted}/{len(all_artifacts)}")
    await check_pg_table_rows(pg_engine)


async def main():
    global DRY_RUN

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Hypha Database Recovery Script - Rebuild SQLite database from S3 backups"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually making changes",
    )
    args = parser.parse_args()

    DRY_RUN = args.dry_run

    if DRY_RUN:
        logger.info("🔍 DRY RUN MODE: No changes will be made to the database")

    SQLModel.metadata.clear()

    env_vars = load_env_file(ENV_FILE) if Path(ENV_FILE).exists() else {}
    s3_config = get_s3_config(env_vars)
    additional_s3_sources = get_additional_s3_sources(
        ADDITIONAL_ENV_FILES, ADDITIONAL_ENV_KEYS
    )

    await rebuild_database(s3_config, additional_s3_sources)

    if not DRY_RUN:
        engine = create_async_engine(DATABASE_URI, echo=False)
        sqlite_session_maker = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )

        await remove_orphan_artifacts(sqlite_session_maker)
        db_password = env_vars.get("DB_PASSWORD", "")
        db_host = env_vars.get("DB_HOST", "")
        db_user = env_vars.get("DB_USER", DB_USER_DEFAULT)
        db_port = env_vars.get("DB_PORT", DB_PORT_DEFAULT)
        db_name = env_vars.get("DB_NAME", DB_NAME_DEFAULT)

        database_url = f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        pg_engine = create_async_engine(database_url, echo=False)
        pg_session_maker = async_sessionmaker(
            pg_engine, expire_on_commit=False, class_=AsyncSession
        )

        await transfer_artifacts_topo(pg_engine, sqlite_session_maker, pg_session_maker)
    else:
        logger.info("🔍 DRY RUN completed - no database operations were performed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hypha Database Recovery Script")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a trial run with no changes made",
    )
    args = parser.parse_args()

    DRY_RUN = args.dry_run

    asyncio.run(main())
