import asyncio
from datetime import datetime
from sqlalchemy import select, text, or_, and_
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlmodel import SQLModel
from utils.artifact_model import ArtifactModel
from utils.constants import (
    DATABASE_URI,
    DATABASE_URL,
    DB_USER_DEFAULT,
    DB_NAME_DEFAULT,
    DB_PORT_DEFAULT,
)

engine = create_async_engine(DATABASE_URI, echo=False)
session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# PostgreSQL engine and session
pg_engine = create_async_engine(DATABASE_URL, echo=False)
pg_session_maker = async_sessionmaker(
    pg_engine, expire_on_commit=False, class_=AsyncSession
)
pg_engine_local = create_async_engine(
    DATABASE_URI, echo=False  # Set echo to False to reduce output
)
pg_session_maker_local = async_sessionmaker(
    pg_engine_local, expire_on_commit=False, class_=AsyncSession
)


async def get_artifact(workspace: str, alias: str):
    async with session_maker() as session:
        stmt = select(ArtifactModel).where(
            ArtifactModel.workspace == workspace, ArtifactModel.alias == alias
        )
        result = await session.exec(stmt)
        return result.one_or_none()


async def list_children(parent_id: str):
    async with session_maker() as session:
        stmt = select(ArtifactModel).where(ArtifactModel.parent_id == parent_id)
        children = await session.exec(stmt)
        children = children.all()
        print(f"🔗 Found {len(children)} child artifacts:")
        for child in children:
            print(
                f" - {child.alias} ({child.id}), workspace: {child.workspace}, file_count: {child.file_count}, last_modified: {datetime.utcfromtimestamp(child.last_modified).isoformat()}"
            )


async def inspect_collection(workspace: str, alias: str):
    print(f"🔍 Inspecting: {workspace}/{alias}")
    artifact = await get_artifact(workspace, alias)
    if not artifact:
        print("❌ Artifact not found.")
        return

    print(f"🧬 ID: {artifact.id}")
    print(f"📦 Created at: {datetime.utcfromtimestamp(artifact.created_at)}")
    print(f"🛠️  File count: {artifact.file_count}")
    print(f"👤 Created by: {artifact.created_by}")
    print(
        f"📝 Manifest name: {artifact.manifest.get('name') if artifact.manifest else 'N/A'}"
    )
    print(f"📅 Last modified: {datetime.utcfromtimestamp(artifact.last_modified)}")

    await list_children(artifact.id)


# Replace with your PostgreSQL session maker
async def test_raw_list_children(parent_id: str):
    async with pg_session_maker() as session:
        print(f"🔍 Looking for children of artifact: {parent_id}")

        query = select(ArtifactModel).where(ArtifactModel.parent_id == parent_id)
        result = await session.execute(query)
        children = result.scalars().all()

        print(f"✅ Found {len(children)} children")
        for a in children[:10]:  # Preview first 10
            created = (
                datetime.utcfromtimestamp(a.created_at).isoformat()
                if a.created_at
                else "?"
            )
            print(
                f" - {a.id}: {a.alias} (created: {created}, file_count: {a.file_count})"
            )


async def list_children_test(
    session: AsyncSession, parent_alias: str, workspace: str
) -> list[ArtifactModel]:
    """
    Test function to list children artifacts for a given parent alias in a workspace.
    """
    # Step 1: Get the parent artifact
    result = await session.execute(
        select(ArtifactModel).where(
            ArtifactModel.alias == parent_alias, ArtifactModel.workspace == workspace
        )
    )
    parent = result.scalar_one_or_none()

    if not parent:
        print(f"❌ Parent artifact not found: {workspace}/{parent_alias}")
        return []

    print(f"📁 Found parent artifact: {parent.id} ({parent.alias})")

    # Step 2: Get children artifacts
    result = await session.execute(
        select(ArtifactModel).where(ArtifactModel.parent_id == parent.id)
    )
    children = result.scalars().all()

    print(f"🔍 Found {len(children)} children artifacts for {parent.alias}:")
    for child in children:
        created_at = datetime.datetime.fromtimestamp(child.created_at).isoformat()
        print(
            f" - {child.id}: {child.alias} (created: {created_at}, file_count: {child.file_count})"
        )

    return children


async def debug_list_children_like(
    parent_id: str, stage=None, keyword=None, config_permission_filter=None
):
    async with pg_session_maker() as session:
        print(f"🔍 Testing children of parent: {parent_id}")
        backend = pg_engine.dialect.name
        query = select(ArtifactModel).where(ArtifactModel.parent_id == parent_id)

        # Simulate `stage` filter
        if stage is not None:
            print(f"⚙️ Adding stage filter: {stage}")
            if stage:
                query = query.where(
                    and_(
                        ArtifactModel.staging.isnot(None),
                        text("staging::text != 'null'"),
                    )
                )
            else:
                query = query.where(
                    or_(ArtifactModel.staging.is_(None), text("staging::text = 'null'"))
                )

        # Simulate keyword filter
        if keyword:
            print(f"🔍 Adding keyword filter: '{keyword}'")
            query = query.where(text(f"manifest::text ILIKE '%{keyword}%'"))

        # Simulate config.permissions filter
        if config_permission_filter:
            print(f"🔐 Adding permission filter: {config_permission_filter}")
            for user_id, permission in config_permission_filter.items():
                condition = text(
                    f"config->'permissions'->>'{user_id}' = '{permission}'"
                )
                query = query.where(condition)

        query = query.order_by(ArtifactModel.created_at.asc()).limit(10)
        result = await session.execute(query)
        children = result.scalars().all()

        print(f"✅ Found {len(children)} children")
        for a in children:
            created = datetime.utcfromtimestamp(a.created_at).isoformat()
            print(
                f" - {a.id}: {a.alias} (created: {created}, file_count: {a.file_count})"
            )


async def test_pg_list_children(parent_id: str, stage: bool = None, limit: int = 10):
    print(f"\n🔍 Looking for children of artifact: {parent_id}")
    async with pg_session_maker() as session:
        stmt = select(ArtifactModel).where(ArtifactModel.parent_id == parent_id)

        # Safe cross-database filter on staging status
        if stage is True:
            stmt = stmt.where(ArtifactModel.staging.isnot(None))
        elif stage is False:
            stmt = stmt.where(ArtifactModel.staging.is_(None))

        stmt = stmt.order_by(ArtifactModel.created_at.asc()).limit(limit)

        result = await session.execute(stmt)
        children = result.scalars().all()

        print(f"✅ Found {len(children)} children")
        for a in children:
            ts = datetime.fromtimestamp(a.created_at).isoformat()
            print(f" - {a.id}: {a.alias} (created: {ts}, file_count: {a.file_count})")


async def list_children_pg_test(
    parent_id: str,
    workspace: str,
    keywords: list = None,
    filters: dict = None,
    stage: bool = None,
    order_by: str = "created_at<",
    limit: int = 10,
    offset: int = 0,
):
    print(f"\n🔍 Looking for children of artifact: {parent_id}")
    # PostgreSQL engine and session
    pg_engine = create_async_engine(DATABASE_URL, echo=False)
    print(DATABASE_URL)
    pg_session_maker = async_sessionmaker(
        pg_engine, expire_on_commit=False, class_=AsyncSession
    )

    async with pg_session_maker() as session:
        query = select(ArtifactModel).where(ArtifactModel.parent_id == parent_id)
        conditions = []

        # 🔍 Keyword matching (manifest text search)
        if keywords:
            for keyword in keywords:
                conditions.append(text(f"manifest::text ILIKE '%{keyword}%'"))

        # 🎯 Filter matching
        if filters:
            for key, value in filters.items():
                if key == "type":
                    conditions.append(ArtifactModel.type == value)
                elif key == "created_by":
                    conditions.append(ArtifactModel.created_by == value)
                elif key == "alias":
                    conditions.append(ArtifactModel.alias == value)
                elif key == "manifest" and isinstance(value, dict):
                    for k, v in value.items():
                        conditions.append(text(f"manifest->>'{k}' = '{v}'"))
                elif key == "config" and isinstance(value, dict):
                    for k, v in value.items():
                        conditions.append(text(f"config->>'{k}' = '{v}'"))

        # ✅ Stage filter (PostgreSQL-safe)
        if stage is not None:
            if stage:
                stage_condition = and_(
                    ArtifactModel.staging.isnot(None), text("staging::text != 'null'")
                )
            else:
                stage_condition = or_(
                    ArtifactModel.staging.is_(None), text("staging::text = 'null'")
                )
            conditions.append(stage_condition)

        # Combine all conditions
        if conditions:
            query = query.where(and_(*conditions))

        # Sort order parsing
        order_field_map = {
            "id": ArtifactModel.id,
            "created_at": ArtifactModel.created_at,
            "last_modified": ArtifactModel.last_modified,
            "download_count": ArtifactModel.download_count,
            "view_count": ArtifactModel.view_count,
        }
        field_name = order_by.split("<")[0]
        ascending = "<" in order_by
        order_field = order_field_map.get(field_name, ArtifactModel.created_at)
        query = query.order_by(order_field.asc() if ascending else order_field.desc())
        query = query.limit(limit).offset(offset)

        # Execute and format result
        result = await session.execute(query)
        children = result.scalars().all()

        print(f"✅ Found {len(children)} children")
        for a in children:
            ts = datetime.fromtimestamp(a.created_at).isoformat()
            print(f" - {a.id}: {a.alias} (created: {ts}, file_count: {a.file_count})")


# TODO: use pg_engine_local from here


async def check_database_stats(a_session_maker):
    """Query the database and display table statistics without writing data"""
    print("🔍 Checking PostgreSQL database status...")

    try:
        async with a_session_maker() as session:
            # Get list of all tables
            result = await session.execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            )
            tables = result.scalars().all()

            print(f"📊 Database contains {len(tables)} tables:")
            for table in tables:
                print(f"  - {table}")

                # Get count of rows per table
                count_query = text(f"SELECT COUNT(*) FROM {table}")
                count_result = await session.execute(count_query)
                row_count = count_result.scalar()
                print(f"    • Rows: {row_count}")

                # Get table size
                size_query = text(
                    f"SELECT pg_size_pretty(pg_total_relation_size('{table}'))"
                )
                size_result = await session.execute(size_query)
                table_size = size_result.scalar()
                print(f"    • Size: {table_size}")

                # If it's the artifacts table and it exists, show some stats
                if table == "artifacts":
                    # Count by workspace
                    try:
                        workspace_query = text(
                            "SELECT workspace, COUNT(*) FROM artifacts GROUP BY workspace"
                        )
                        workspace_result = await session.execute(workspace_query)
                        workspace_counts = workspace_result.all()

                        if workspace_counts:
                            print(f"    • Artifacts by workspace:")
                            for workspace, count in workspace_counts:
                                print(f"      - {workspace}: {count} artifacts")
                    except Exception as e:
                        print(f"    • Could not get workspace stats: {str(e)}")

            # Get database size
            db_size_query = text(
                "SELECT pg_size_pretty(pg_database_size(current_database()))"
            )
            db_size_result = await session.execute(db_size_query)
            db_size = db_size_result.scalar()
            print(f"\n💾 Total database size: {db_size}")

            # Get connection info
            version_query = text("SELECT version()")
            version_result = await session.execute(version_query)
            version = version_result.scalar()
            print(f"\n🔌 Connected to: {version}")

            return True

    except Exception as e:
        print(f"❌ Error checking database stats: {str(e)}")
        try_sync_connection()
        return False


async def test_database_write_read(a_session_maker):
    """Test database by creating a temporary table, writing data, reading it back, and dropping the table"""
    print("\n🧪 Testing database write/read operations...")

    test_table_name = "test_hypha_table"

    try:
        async with pg_session_maker() as session:
            # Check if test table exists and drop it to ensure clean test
            check_table = text(f"SELECT to_regclass('public.{test_table_name}')")
            result = await session.execute(check_table)
            if result.scalar():
                print(f"  • Found existing test table, dropping it first")
                await session.execute(text(f"DROP TABLE {test_table_name}"))
                await session.commit()

            # Create test table
            create_table = text(
                f"""
                CREATE TABLE {test_table_name} (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
            await session.execute(create_table)
            await session.commit()
            print(f"  • Created test table: {test_table_name}")

            # Insert test data
            test_data = [("item1", 100), ("item2", 200), ("item3", 300)]

            for name, value in test_data:
                insert = text(
                    f"INSERT INTO {test_table_name} (name, value) VALUES (:name, :value)"
                )
                await session.execute(insert, {"name": name, "value": value})

            await session.commit()
            print(f"  • Inserted {len(test_data)} rows of test data")

            # Read back data to verify
            select = text(f"SELECT id, name, value FROM {test_table_name} ORDER BY id")
            result = await session.execute(select)
            rows = result.fetchall()

            # Verify data integrity
            if len(rows) == len(test_data):
                print(f"  • Successfully read back {len(rows)} rows:")
                for row in rows:
                    print(f"    - ID: {row[0]}, Name: {row[1]}, Value: {row[2]}")

                # Verify values match what we inserted
                all_match = True
                for i, (name, value) in enumerate(test_data):
                    if rows[i][1] != name or rows[i][2] != value:
                        all_match = False
                        break

                if all_match:
                    print("  ✅ Data verification successful - all values match")
                else:
                    print("  ❌ Data verification failed - values don't match")
            else:
                print(
                    f"  ❌ Data count mismatch: inserted {len(test_data)}, read back {len(rows)}"
                )

            # Drop test table
            drop_table = text(f"DROP TABLE {test_table_name}")
            await session.execute(drop_table)
            await session.commit()
            print(f"  • Cleaned up: dropped test table {test_table_name}")

            return True

    except Exception as e:
        print(f"❌ Error during database write/read test: {str(e)}")
        return False


async def try_sync_connection(  # should be sync
    db_user=DB_USER_DEFAULT,
    db_password="",
    db_host="",
    db_port=DB_PORT_DEFAULT,
    db_name=DB_NAME_DEFAULT,
):
    """Fallback to sync connection for troubleshooting"""
    try:
        print("\nTrying sync connection with psycopg2...")
        sync_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        # Create synchronous engine
        sync_engine = create_async_engine(sync_url)  # should be create_engine for sync

        # Test connection
        async with sync_engine.connect() as conn:  # should be sync
            tables = conn.execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            )
            print(f"Tables in database: {[table[0] for table in tables]}")

            db_size = conn.execute(
                text("SELECT pg_size_pretty(pg_database_size(current_database()))")
            )
            print(f"Database size: {db_size.scalar()}")

            version = conn.execute(text("SELECT version()"))
            print(f"Connected to: {version.scalar()}")

        print("✅ Successfully connected with psycopg2!")

    except Exception as e2:
        print(f"❌ Error with sync connection: {str(e2)}")


async def verify_migration():
    print("🔍 Starting migration verification...")

    async with pg_session_maker_local() as sqlite_session, pg_session_maker() as pg_session:
        # Fetch from SQLite
        sqlite_result = await sqlite_session.exec(select(ArtifactModel))
        sqlite_artifacts = {a.id: a for a in sqlite_result.all()}

        # Fetch from PostgreSQL
        pg_result = await pg_session.execute(select(ArtifactModel))
        pg_artifacts = {a.id: a for a in pg_result.scalars().all()}

    total_sqlite = len(sqlite_artifacts)
    total_pg = len(pg_artifacts)
    print(f"📦 SQLite artifacts: {total_sqlite}")
    print(f"📥 PostgreSQL artifacts: {total_pg}")

    missing_in_pg = []
    mismatches = []

    for id_, sa in sqlite_artifacts.items():
        pa = pg_artifacts.get(id_)
        if not pa:
            missing_in_pg.append(id_)
        else:
            # Optional: compare specific fields
            if (
                sa.alias != pa.alias
                or sa.workspace != pa.workspace
                or sa.last_modified != pa.last_modified
            ):
                mismatches.append((id_, sa, pa))

    print(f"❌ Missing in PostgreSQL: {len(missing_in_pg)}")
    print(f"⚠️  Mismatched artifacts: {len(mismatches)}")

    if missing_in_pg:
        print("Missing artifact IDs:")
        for id_ in missing_in_pg[:5]:
            print(f" - {id_}")
        if len(missing_in_pg) > 5:
            print(" ...")

    if mismatches:
        print("Examples of mismatches:")
        for id_, sa, pa in mismatches[:3]:
            print(f" - ID: {id_}")
            print(
                f"   → SQLite: alias={sa.alias}, workspace={sa.workspace}, last_modified={sa.last_modified}"
            )
            print(
                f"   → Postgres: alias={pa.alias}, workspace={pa.workspace}, last_modified={pa.last_modified}"
            )

    print("✅ Verification complete.")


async def minimal_pg_list_children_repro(parent_id: str, stage=None, limit=10):
    async with session_maker() as session:
        query = select(ArtifactModel).where(ArtifactModel.parent_id == parent_id)

        if stage is not None:
            print(f"Applying stage filter: {stage}")
            if stage:
                # staged artifacts
                query = query.where(ArtifactModel.staging.isnot(None))
            else:
                # committed artifacts
                query = query.where(
                    text("json_array_length(versions) > 0")
                    # func.json_array_length(ArtifactModel.versions) > 0
                    # or_(
                    #     ArtifactModel.staging.is_(None),
                    #     text("staging::text = 'null'"),
                    # )
                )
        else:
            print("No stage filter applied")

        query = query.order_by(ArtifactModel.created_at.asc()).limit(limit)

        result = await session.execute(query)
        artifacts = result.scalars().all()

        print(f"\nFound {len(artifacts)} children (stage={stage}):")
        for a in artifacts:
            ts = datetime.fromtimestamp(a.created_at).isoformat()
            print(f" - {a.id}: {a.alias} (created: {ts}, file_count: {a.file_count})")


# stop using pg_engine_local from here


async def drop_artifacts_table(pg_engine):
    async with pg_engine.begin() as conn:
        await conn.run_sync(
            lambda conn: ArtifactModel.__table__.drop(conn, checkfirst=True)
        )
    print("🗑️ Dropped existing 'artifacts' table.")


async def init_postgres_schema(pg_engine):
    async with pg_engine.begin() as conn:
        await conn.run_sync(
            lambda conn: SQLModel.metadata.create_all(conn, checkfirst=True)
        )
    print("✅ PostgreSQL schema initialized.")


async def debug_pg_schema():
    async with pg_engine.begin() as conn:
        result = await conn.execute(
            text(
                """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
        """
            )
        )
        rows = result.fetchall()
        for schema, table in rows:
            print(f"📦 Found table: {schema}.{table}")


async def inspect_artifacts():
    async with pg_engine.begin() as conn:
        result = await conn.execute(text("SELECT COUNT(*) FROM artifacts"))
        count = result.scalar()
        print(f"📊 Total artifacts in PostgreSQL: {count}")

        # Show 5 example entries
        result = await conn.execute(
            text(
                """
            SELECT id, alias, workspace, file_count, created_at, last_modified
            FROM artifacts
            ORDER BY last_modified DESC
            LIMIT 5
        """
            )
        )
        rows = result.fetchall()
        for row in rows:
            print("📄", row)


async def main():
    # Run both inspections
    await inspect_collection("bioimage-io", "bioimage.io")
    print("\n" + "-" * 60 + "\n")
    await inspect_collection("shareloc-xyz", "shareloc-collection")
    await test_raw_list_children("019362db-e4b7-7fd1-92b3-dd451eefeaa1")  # bioimage.io

    # ✅ Works: no filters
    await debug_list_children_like("019362db-e4b7-7fd1-92b3-dd451eefeaa1")

    # 🔍 Test stage=False filter (should work if correctly implemented)
    await debug_list_children_like("019362db-e4b7-7fd1-92b3-dd451eefeaa1", stage=False)

    # 🔍 Test stage=True filter (may return fewer or none)
    await debug_list_children_like("019362db-e4b7-7fd1-92b3-dd451eefeaa1", stage=True)

    # 🧪 Test keyword filter — replace with a known string if needed
    await debug_list_children_like(
        "019362db-e4b7-7fd1-92b3-dd451eefeaa1", keyword="crab"
    )

    # 🔐 Simulate permission filter
    await debug_list_children_like(
        "019362db-e4b7-7fd1-92b3-dd451eefeaa1",
        config_permission_filter={"sophisticated-humidity-98268842": "*"},
    )

    parent_id = "019362db-e4b7-7fd1-92b3-dd451eefeaa1"

    await test_pg_list_children(parent_id, stage=None)
    await test_pg_list_children(parent_id, stage=True)
    await test_pg_list_children(parent_id, stage=False)

    await list_children_pg_test(
        parent_id="019362db-e4b7-7fd1-92b3-dd451eefeaa1",
        workspace="bioimage-io",
        keywords=["deep"],
        filters={"type": "model"},
        stage=False,
        limit=10,
    )
    print("🔄 Starting database connection test...")
    stats_success = await check_database_stats(pg_session_maker)
    await verify_migration()
    async with pg_session_maker() as session:
        await list_children_test(session, "bioimage.io", "bioimage-io")
    await debug_pg_schema()
    test_parent_id = "019362db-e4b7-7fd1-92b3-dd451eefeaa1"
    await minimal_pg_list_children_repro(test_parent_id, stage=None)
    await minimal_pg_list_children_repro(test_parent_id, stage=True)
    await minimal_pg_list_children_repro(test_parent_id, stage=False)
    await inspect_artifacts()

    limit = 10
    async with pg_session_maker_local() as session:
        print("📡 Connecting to PostgreSQL and querying artifacts...")
        result = await session.execute(
            select(ArtifactModel)
            .order_by(ArtifactModel.last_modified.desc())
            .limit(limit)
        )
        artifacts = result.all()
        print(f"✅ Retrieved {len(artifacts)} artifacts:\n")
        for (art,) in artifacts:
            created = datetime.utcfromtimestamp(art.created_at).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            modified = datetime.utcfromtimestamp(art.last_modified).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            print(
                f"📦 {art.workspace}/{art.alias or art.id} — files: {art.file_count}, created: {created}, modified: {modified}"
            )


if __name__ == "__main__":
    asyncio.run(main())
