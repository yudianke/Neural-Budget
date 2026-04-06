import os
from pathlib import Path

from swiftclient.client import Connection


def get_swift_connection() -> Connection:
    return Connection(
        authurl=os.environ["OS_AUTH_URL"],
        auth_version="3",
        os_options={
            "auth_type": "v3applicationcredential",
            "application_credential_id": os.environ["OS_APPLICATION_CREDENTIAL_ID"],
            "application_credential_secret": os.environ["OS_APPLICATION_CREDENTIAL_SECRET"],
            "region_name": os.environ.get("OS_REGION_NAME", "CHI@TACC"),
        },
    )


def create_container_if_not_exists(conn: Connection, container_name: str) -> None:
    existing = [c["name"] for c in conn.get_account()[1]]
    if container_name not in existing:
        conn.put_container(container_name)
        print(f"Created container: {container_name}")
    else:
        print(f"Container already exists: {container_name}")


def upload_file(conn: Connection, container_name: str, file_path: Path, object_name: str) -> None:
    with open(file_path, "rb") as f:
        conn.put_object(container_name, object_name, contents=f)
    print(f"Uploaded {file_path} -> {container_name}/{object_name}")


def main() -> None:
    container_name = os.environ["SWIFT_CONTAINER_NAME"]
    processed_dir = Path("data/processed")

    conn = get_swift_connection()
    create_container_if_not_exists(conn, container_name)

    for file in processed_dir.rglob("*"):
        if file.is_file():
            upload_file(conn, container_name, file, f"processed/{file.name}")


if __name__ == "__main__":
    main()
