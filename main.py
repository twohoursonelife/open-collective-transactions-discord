import sqlite3
import os
import requests
import pandas as pd
from dotenv import load_dotenv
from discord import SyncWebhook
from datetime import datetime, timedelta, UTC

load_dotenv()

WEBHOOK_URL = os.getenv("WEBHOOK_URL")
OC_API_KEY = os.getenv("OC_API_KEY")

OC_ACCOUNT_SLUG = os.getenv("OC_ACCOUNT_SLUG", "twohoursonelife")
OC_API_ENDPOINT = "https://api.opencollective.com/graphql/v2"
RFC3339_ISO8601_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
SQLITE3_PATH = "financials.db"
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "200"))

SQL_CONNECTION = sqlite3.connect(SQLITE3_PATH)


def main() -> None:
    drop_transaction_table(SQL_CONNECTION) # Debug
    setup_database(SQL_CONNECTION)

    lookback_time = datetime.now(UTC) - timedelta(hours=LOOKBACK_HOURS)

    latest_transactions = get_open_collective_transactions(lookback_time)
    known_transactions = get_known_transactions(lookback_time, SQL_CONNECTION)

    new_transactions = find_new_transactions(latest_transactions, known_transactions)

    if not new_transactions.empty:
        save_transactions(new_transactions, SQL_CONNECTION)
        send_discord_transactions(new_transactions)
        print(f"Saved and sent {len(new_transactions)} new transactions.")
        return
    
    print(f"No new transactions to send.")


def sql_query(query: str, connection: sqlite3.Connection) -> list:
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    return cursor.fetchall()


def setup_database(connection: sqlite3.Connection) -> None:
    setup_transaction_table(connection)


def setup_transaction_table(connection: sqlite3.Connection) -> None:
    TRANSACTIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS transactions (
        id TEXT PRIMARY KEY,
        created_at TEXT,
        from_account TEXT,
        amount_cents INTERGER);
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


def query_open_collective(query: str, variables: str) -> requests.Response:
    payload = {"query": query, "variables": variables}
    headers = {"Content-Type": "application/json", "Api-key": OC_API_KEY}

    response = requests.post(OC_API_ENDPOINT, json=payload, headers=headers)
    response.raise_for_status()

    json = response.json()

    if "errors" in json:
        raise Exception("Error during OC query", json.get("errors"))

    return response


def get_open_collective_transactions(start_date) -> pd.DataFrame:
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
        "SELECT * FROM transactions WHERE created_at > ?",
        connection,
        parse_dates=["created_at"],
        params=[lookback_time],
    )


def send_discord_transactions(transactions: pd.DataFrame) -> None:
    message = ""
    # Reversed order to match chronological order of messages.
    for index, row in transactions[::-1].iterrows():

        dollar_amount = row["amount_cents"] / 100
        donor = row["from_account"]
        donation_time = int(row["created_at"].floor("s").timestamp())

        message += f"Thank you **{donor}** for your contribution of **${dollar_amount:.2f}**, <t:{donation_time}:R>! <:love:1104682659537485844>\n"

    if len(message) < 1:
        return

    if len(message) >= 2000:
        raise Exception("Edge case, too many donors!")

    webhook = SyncWebhook.from_url(WEBHOOK_URL)

    time = datetime.now()
    webhook.send(
        content=message,
        username="Open Collective",
        avatar_url="https://cdn.discordapp.com/avatars/948569181513724024/dab2d4c4ae7f5b2253a97dde3ef09a67.webp?size=80",
    )
    print(f"Took {datetime.now() - time} to send Discord webhook.")


if __name__ == "__main__":
    main()
