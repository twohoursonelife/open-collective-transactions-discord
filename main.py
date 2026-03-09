import sqlite3
import os
import requests
import pandas as pd
from dotenv import load_dotenv
from discord import SyncWebhook
from datetime import datetime, timedelta, UTC
import boto3
from botocore.exceptions import ClientError

load_dotenv()

WEBHOOK_URL = os.getenv("WEBHOOK_URL")
OC_API_KEY = os.getenv("OC_API_KEY")

OC_ACCOUNT_SLUG = os.getenv("OC_ACCOUNT_SLUG", "twohoursonelife")
OC_API_ENDPOINT = "https://api.opencollective.com/graphql/v2"
RFC3339_ISO8601_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
SQLITE3_PATH = "financials.db"
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "12"))

ENABLE_REMOTE_DB = os.getenv("ENABLE_REMOTE_DB", "true") == "true"
BUCKET_NAME = os.getenv("BUCKET_NAME", "financials")
AWS_ENDPOINT_URL_S3 = os.getenv("AWS_ENDPOINT_URL_S3")

# Implicit
# AWS_ACCESS_KEY_ID
# AWS_SECRET_ACCESS_KEY
# AWS_REGION


def main() -> None:
    sql_connection = prepare_database()

    lookback_time = get_latest_transaction_time(sql_connection)

    if lookback_time is None:
        lookback_time = datetime.now(UTC) - timedelta(hours=LOOKBACK_HOURS)

    latest_transactions = get_open_collective_transactions(lookback_time)
    known_transactions = get_known_transactions(lookback_time, sql_connection)

    new_transactions = find_new_transactions(latest_transactions, known_transactions)

    if new_transactions.empty:
        print("No new transactions to process.")
        return

    # We want notification exceptions to occur before recording the transaction as processed.
    send_discord_transactions(new_transactions)
    save_transactions(new_transactions, sql_connection)

    cleanup_database()

    print(f"Processed {len(new_transactions)} new transactions.")


def prepare_database() -> sqlite3.Connection:
    if not os.path.exists(SQLITE3_PATH) and ENABLE_REMOTE_DB:
        s3_client = boto3.client(
            "s3",
            endpoint_url=AWS_ENDPOINT_URL_S3,
        )

        try:
            s3_client.download_file(BUCKET_NAME, "financials.db", SQLITE3_PATH)
            print("Downloaded DB.")

        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print("DB not found in bucket. Starting fresh.")
            else:
                raise

    sql_connection = sqlite3.connect(SQLITE3_PATH)
    setup_database(sql_connection)

    return sql_connection


def cleanup_database() -> None:
    if ENABLE_REMOTE_DB:
        s3_client = boto3.client(
            "s3",
            endpoint_url=AWS_ENDPOINT_URL_S3,
        )

        s3_client.upload_file(SQLITE3_PATH, BUCKET_NAME, "financials.db")
        print("Uploaded DB")


def sql_query(query: str, connection: sqlite3.Connection) -> list:
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    return cursor.fetchall()


def get_latest_transaction_time(connection: sqlite3.Connection) -> datetime | None:
    res = sql_query("SELECT MAX(created_at) FROM transactions", connection)
    if res and res[0][0]:
        return pd.to_datetime(res[0][0])
    return None


def setup_database(connection: sqlite3.Connection) -> None:
    setup_transaction_table(connection)


def setup_transaction_table(connection: sqlite3.Connection) -> None:
    TRANSACTIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS transactions (
        id TEXT PRIMARY KEY,
        created_at TEXT,
        from_account TEXT,
        amount_cents INTEGER);
    """

    sql_query(
        TRANSACTIONS_TABLE,
        connection,
    )


def drop_transaction_table(connection: sqlite3.Connection) -> None:
    sql_query(
        "DROP TABLE transactions",
        connection,
    )


def delete_all_transactions(connection: sqlite3.Connection) -> None:
    sql_query(
        "DELETE FROM transactions",
        connection,
    )


def add_dummy_new_transactions(data: pd.DataFrame) -> pd.DataFrame:
    dummy_transactions = pd.DataFrame(
        [
            [
                "b6c1d908-caf5-4f56-89b1-814b4f1f7d46",
                "2025-01-28T13:06:38.414Z",
                "Hope",
                2000,
            ]
        ],
        columns=["id", "created_at", "from_account", "amount_cents"],
    )

    return pd.concat(
        [data, dummy_transactions],
        ignore_index=True,
    )


def query_open_collective(query: str, variables: dict) -> requests.Response:
    payload = {"query": query, "variables": variables}
    headers = {"Content-Type": "application/json", "Api-key": OC_API_KEY}

    response = requests.post(OC_API_ENDPOINT, json=payload, headers=headers)
    response.raise_for_status()

    json = response.json()

    if "errors" in json:
        raise Exception("Error during OC query", json.get("errors"))

    return response


def get_open_collective_transactions(start_date: datetime) -> pd.DataFrame:
    QUERY = """
    query account($account: String, $start_date: DateTime) {
        account(slug: $account) {
            name
            slug
            transactions(limit: 1000, type: CREDIT, dateFrom: $start_date) {
                totalCount
                nodes {
                    id
                    fromAccount {
                        name
                    }
                    amount {
                        valueInCents
                    }
                    createdAt
                }
            }
        }
    }
    """

    VARIABLES = {
        "account": OC_ACCOUNT_SLUG,
        "start_date": start_date.strftime(RFC3339_ISO8601_DATETIME_FORMAT),
    }

    response = query_open_collective(
        QUERY,
        VARIABLES,
    )

    contributions = pd.json_normalize(
        response.json()["data"]["account"]["transactions"]["nodes"]
    )

    contributions.rename(
        columns={
            "createdAt": "created_at",
            "fromAccount.name": "from_account",
            "amount.valueInCents": "amount_cents",
        },
        inplace=True,
    )

    if not contributions.empty:
        contributions["created_at"] = pd.to_datetime(contributions["created_at"])

    return contributions


def save_transactions(
    transactions: pd.DataFrame, connection: sqlite3.Connection
) -> None:
    transactions.to_sql(
        "transactions",
        connection,
        if_exists="append",
        index=False,
        method="multi",
    )


def find_new_transactions(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    if left.empty:
        return pd.DataFrame()

    return left[~left["id"].isin(right["id"])]


def get_known_transactions(
    lookback_time: datetime, connection: sqlite3.Connection
) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT * FROM transactions WHERE created_at >= ?",
        connection,
        parse_dates=["created_at"],
        params=[str(lookback_time)],
    )


def send_discord_transactions(transactions: pd.DataFrame) -> None:
    messages = []
    current_message = ""
    # Reversed order to match chronological order of messages.
    for index, row in transactions[::-1].iterrows():
        dollar_amount = row["amount_cents"] / 100
        donor = row["from_account"]
        donation_time = int(row["created_at"].floor("s").timestamp())

        line = f"Thank you **{donor}** for your contribution of **${dollar_amount:.2f}**, <t:{donation_time}:R>! <:love:1104682659537485844>\n"

        if len(current_message) + len(line) >= 2000:
            messages.append(current_message)
            current_message = line
        else:
            current_message += line

    if current_message:
        messages.append(current_message)

    if not messages:
        return

    webhook = SyncWebhook.from_url(WEBHOOK_URL)

    for msg in messages:
        webhook.send(
            content=msg,
            username="Two Hours One Life - Open Collective",
            avatar_url="https://cdn.discordapp.com/icons/423293333864054833/a_52ab7acbcff9db8362d80364d4d88667.png?size=80",
        )


if __name__ == "__main__":
    main()
