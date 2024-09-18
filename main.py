import csv

from tqdm import tqdm
from neo4j import GraphDatabase

URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "neo4j")


def load_data(file_path: str) -> list[str]:
    with open(file_path) as FH:
        reader = csv.reader(FH)
        next(reader)
        rows = [row for row in reader]

        return rows


def load_actor_details(driver, actor_chunk: list[list[str]]):
    records = []
    for actor_data in actor_chunk:
        name = actor_data[0]
        actor_id = actor_data[1]
        film = actor_data[2]
        year = actor_data[3]
        film_id = actor_data[6]
        record = dict(
            name=name,
            actor_id=actor_id,
            film=film,
            year=year,
            film_id=film_id,
        )
        records.append(record)

    driver.execute_query(
        """
        UNWIND $records as record
        MERGE (act:Actor {name: record.name, actor_id: record.actor_id})
        MERGE (film:Film {name: record.film, year: record.year, film_id: record.film_id})
        MERGE (act)-[:ACTED_IN]->(film)
        """,
        records=records
    )


def chunk_records(records, chunk_size: int = 1000):
    chunks = []
    chunk_count = (len(records) // chunk_size) + 1  # +1 to account for last chunk that may not be a perfect chunk size

    for chunk_idx in range(chunk_count):
        start_idx = chunk_idx * chunk_size
        end_idx = (chunk_idx * chunk_size) + chunk_size + 1
        chunk_recs = records[start_idx:end_idx]
        chunks.append(chunk_recs)

    return chunks



def main():
    actor_rows = load_data("./actorfilms.csv")
    row_chunks = chunk_records(actor_rows)
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        for row_chunk in tqdm(row_chunks):
            load_actor_details(driver, row_chunk)


if __name__ == "__main__":
    main()
