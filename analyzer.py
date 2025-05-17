import json
import os
import sys
from collections import Counter
from pathlib import Path

import nltk
import pandas as pd
import psycopg2
import tmdbsimple as tmdb
import typer
import warnings
from dotenv import load_dotenv
from nltk.corpus import stopwords
from rich.console import Console
from rich.progress import track
from rich.table import Table
from scipy.stats import pearsonr
from wordcloud import WordCloud

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable.*",
    category=UserWarning,
)

load_dotenv()
console = Console()
app = typer.Typer(add_completion=False)

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://demo_user:demo_pass@localhost:5432/demo_db"
)
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
if not TMDB_API_KEY:
    console.print("[red]TMDB_API_KEY missing – set it in .env[/red]")
    sys.exit(1)
tmdb.API_KEY = TMDB_API_KEY

nltk.download("punkt", quiet=True)
nltk.download("stopwords", quiet=True)
STOPWORDS = set(stopwords.words("english"))


def db_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(DATABASE_URL)


@app.command()
def download_movies(destination_directory: str = "data") -> None:
    console.rule("[bold cyan]Downloading Movies[/bold cyan]")
    try:
        from kaggle import api as kaggle_api
    except (ModuleNotFoundError, OSError):
        console.print(
            "[red]Kaggle API is not configured (missing kaggle.json or "
            "KAGGLE_USERNAME/KAGGLE_KEY variables). Either configure it or skip download_movies[/red]"
        )
        raise typer.Exit(code=1)

    kaggle_api.dataset_download_files(
        "asaniczka/tmdb-movies-dataset-2023-930k-movies",
        path=destination_directory,
        unzip=True,
    )


@app.command()
def load_movies(
    json_file: str = "data/tmdb_movies_dump.jsonl",
    max_records: int = 1_000_000,
    max_western_records: int = 2_000,
) -> None:
    console.rule("[bold cyan]Loading Movies[/bold cyan]")
    row_count = 0
    movie_buffer = []
    genre_links = []

    with (
        db_connect() as db_connection,
        db_connection.cursor() as db_cursor,
        open(json_file, "r", encoding="utf-8") as file_handle,
    ):
        for line in file_handle:
            if row_count >= max_records:
                break
            row_count += 1

            try:
                movie_data = json.loads(line)
            except json.JSONDecodeError:
                continue

            if not any(
                genre_item["name"].lower() == "western"
                for genre_item in movie_data["genres"]
            ):
                continue

            production_countries = [
                country_entry["iso_3166_1"] for country_entry in movie_data["production_countries"]
            ]
            if not {"US", "IT"} & set(production_countries):
                continue

            movie_buffer.append(
                (
                    movie_data["id"],
                    movie_data["title"],
                    movie_data["original_title"],
                    movie_data["original_language"],
                    movie_data.get("adult", False),
                    movie_data["status"],
                    movie_data["tagline"],
                    movie_data["overview"],
                    movie_data.get("release_date") or None,
                    movie_data["runtime"],
                    movie_data.get("budget", 0),
                    movie_data.get("revenue", 0),
                    movie_data.get("popularity", 0),
                    movie_data.get("vote_count", 0),
                    movie_data.get("vote_average", 0),
                    movie_data["poster_path"],
                    movie_data["backdrop_path"],
                    movie_data["homepage"],
                    movie_data["imdb_id"],
                    json.dumps(movie_data.get("external_ids", {})),
                    production_countries,
                    json.dumps(movie_data.get("belongs_to_collection")),
                )
            )
            genre_links.append((movie_data["id"], movie_data["genres"]))

        movie_buffer = sorted(
            movie_buffer,
            key=lambda record: (-record[12], -record[13])
        )[:max_western_records]

        db_cursor.executemany(
            """
            INSERT INTO movies(
                tmdb_id, title, original_title, original_language, adult, status, tagline,
                overview, release_date, runtime, budget, revenue, popularity, vote_count,
                vote_average, poster_path, backdrop_path, homepage, imdb_id, external_ids,
                origin_country, collection_id, crew
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s,
                (SELECT id
                   FROM collections
                  WHERE tmdb_id = ((%s)::jsonb->>'id')::int
                  LIMIT 1),
                '[]'::jsonb
            )
            ON CONFLICT (tmdb_id) DO NOTHING
            """,
            [
                (*record[:21], record[21])
                for record in movie_buffer
            ],
        )

        all_genre_names = {genre_item["name"] for _, genre_list in genre_links for genre_item in genre_list}
        db_cursor.executemany(
            "INSERT INTO genres(name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
            [(genre_name,) for genre_name in all_genre_names],
        )

        db_cursor.execute("SELECT id, name FROM genres")
        genre_name_to_id = {name: genre_id for genre_id, name in db_cursor.fetchall()}

        pivot_rows_for_movie_genres = []
        for tmdb_id, genre_list in genre_links:
            db_cursor.execute("SELECT id FROM movies WHERE tmdb_id=%s", (tmdb_id,))
            movie_row = db_cursor.fetchone()
            if not movie_row:
                continue
            movie_row_id = movie_row[0]
            for genre_item in genre_list:
                genre_id = genre_name_to_id.get(genre_item["name"])
                if genre_id:
                    pivot_rows_for_movie_genres.append((movie_row_id, genre_id))

        db_cursor.executemany(
            """
            INSERT INTO movie_genres(movie_id, genre_id)
               VALUES (%s, %s) ON CONFLICT DO NOTHING
            """,
            pivot_rows_for_movie_genres,
        )

        db_connection.commit()
        console.print(
            f"[green]{len(movie_buffer)} Western movies imported, genres linked[/green]"
        )


def fetch_western_movie_pairs() -> list[tuple[int, int]]:
    with db_connect() as db_connection, db_connection.cursor() as db_cursor:
        db_cursor.execute(
            """
            SELECT m.id, m.tmdb_id
              FROM movies m
              JOIN movie_genres mg ON m.id = mg.movie_id
              JOIN genres g        ON g.id = mg.genre_id
             WHERE g.name = 'Western'
               AND m.tmdb_id IS NOT NULL
            """
        )
        return [(row[0], row[1]) for row in db_cursor.fetchall()]


@app.command()
def enrich_crew(
    request_batch_size: int = 50,
    output_dump_path: str = "data/credits_dump.jsonl",
) -> None:
    console.rule("[bold cyan]Enriching Crew Data[/bold cyan]")
    western_movie_pairs = fetch_western_movie_pairs()
    if not western_movie_pairs:
        console.print("[red]No westerns found in the database[/red]")
        raise typer.Exit(1)

    output_path = Path(output_dump_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    dead_file_path = output_path.with_suffix(".dead")
    dead_movie_ids: set[int] = set()
    if dead_file_path.exists():
        for line in dead_file_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                dead_movie_ids.add(int(line.strip()))

    credits_cache: dict[int, dict] = {}
    if output_path.exists():
        with output_path.open("r", encoding="utf-8") as dump_file_handle:
            for line in dump_file_handle:
                try:
                    credit_record = json.loads(line)
                    credits_cache[int(credit_record["tmdb_id"])] = credit_record
                except Exception:
                    continue

    update_count = 0
    with (
        db_connect() as db_connection,
        db_connection.cursor() as db_cursor,
        output_path.open("a", encoding="utf-8") as dump_file_handle,
        dead_file_path.open("a", encoding="utf-8") as dead_file_handle,
    ):
        for database_movie_id, tmdb_movie_id in track(western_movie_pairs, description="Fetching credits…"):
            if tmdb_movie_id in dead_movie_ids:
                continue

            if tmdb_movie_id in credits_cache:
                credits_data = credits_cache[tmdb_movie_id]["raw"]
            else:
                try:
                    credits_response = tmdb.Movies(tmdb_movie_id).credits()
                except Exception as exc:
                    status_code = getattr(exc, "status_code", None)
                    error_message = str(exc).lower()
                    is_not_found_error = (
                        status_code == 404
                        or status_code == 34
                        or "404 client error" in error_message
                        or "status_code: 34" in error_message
                        or "the resource you requested could not be found" in error_message
                    )

                    if is_not_found_error:
                        dead_file_handle.write(f"{tmdb_movie_id}\n")
                        dead_movie_ids.add(tmdb_movie_id)
                    else:
                        console.print(
                            f"[yellow]TMDB error for {tmdb_movie_id} (status: {status_code}): {exc}[/yellow]"
                        )
                    continue

                credit_record = {
                    "tmdb_id": tmdb_movie_id,
                    "directors": [
                        {"role": "director", "name": person_entry["name"]}
                        for person_entry in credits_response["crew"]
                        if person_entry["job"] == "Director" and person_entry["name"]
                    ],
                    "raw": credits_response,
                }
                dump_file_handle.write(json.dumps(credit_record, ensure_ascii=False) + "\n")
                credits_cache[tmdb_movie_id] = credit_record
                credits_data = credits_response

            directors_list = [
                {"role": "director", "name": person_entry["name"]}
                for person_entry in credits_data["crew"]
                if person_entry["job"] == "Director" and person_entry["name"]
            ]
            if not directors_list:
                continue

            db_cursor.execute(
                "UPDATE movies SET crew = %s WHERE id = %s",
                (json.dumps(directors_list), database_movie_id),
            )
            update_count += 1
            if update_count % request_batch_size == 0:
                db_connection.commit()

        db_connection.commit()

    console.print(
        f"[green]Updated {update_count} movies. Cache: {output_path}, skipped (dead): {dead_file_path}[/green]"
    )


@app.command()
def analyze_westerns(output_directory: str = "reports") -> None:
    console.rule("[bold cyan]Analyzing Westerns[/bold cyan]")
    Path(output_directory).mkdir(exist_ok=True)

    console.print("[cyan]Starting analysis of Westerns..[/cyan]")
    with db_connect() as db_connection:
        sql_query = """
            SELECT  
                m.title,
                m.runtime,
                m.release_date,
                m.overview,
                m.popularity
            FROM movies m
            JOIN movie_genres mg ON m.id = mg.movie_id
            JOIN genres g        ON g.id = mg.genre_id
            WHERE g.name = 'Western'
              AND m.release_date IS NOT NULL
        """
        analysis_df = pd.read_sql(sql_query, db_connection)

    if analysis_df.empty:
        console.print(
            "[yellow]No Westerns found in the database for analysis[/yellow]"
        )
        return

    analysis_df["year"] = pd.to_datetime(
        analysis_df["release_date"], errors="coerce"
    ).dt.year
    analysis_df.dropna(subset=["year"], inplace=True)
    analysis_df["year"] = analysis_df["year"].astype(int)

    counts_by_year_series = analysis_df["year"].value_counts().sort_index()
    console.print("[cyan]Analyzing distribution of Westerns by year..[/cyan]")
    counts_by_year_series.to_csv(Path(output_directory) / "westerns_by_year.csv")
    console.print(
        f"[green]Report on Westerns count per year saved to {Path(output_directory) / 'westerns_by_year.csv'}[/green]"
    )

    analysis_df["title_length"] = analysis_df["title"].str.len()
    filtered_dataframe = analysis_df.dropna(subset=["runtime", "title_length"])
    if len(filtered_dataframe) > 2:
        correlation_coefficient, p_value = pearsonr(
            filtered_dataframe["runtime"], filtered_dataframe["title_length"]
        )
        console.print(
            "[cyan]Calculating correlation between movie runtime and title length..[/cyan]"
        )
        with open(Path(output_directory) / "correlation_runtime_title.txt", "w") as correlation_file_handle:
            correlation_file_handle.write(
                f"Pearson r (runtime vs title length): {correlation_coefficient:.3f}, p={p_value:.3g}\n"
            )
        console.print(
            f"[green]Report on runtime vs. title length correlation saved to {Path(output_directory) / 'correlation_runtime_title.txt'}[/green]"
        )
    else:
        console.print(
            "[yellow]Not enough data for runtime vs title length correlation[/yellow]"
        )

    def generate_wordcloud(text_series, suffix_label):
        tokens = []
        for text_item in text_series.dropna():
            if isinstance(text_item, str):
                tokens.extend(
                    word_token
                    for word_token in nltk.word_tokenize(text_item.lower())
                    if word_token.isalpha() and word_token not in STOPWORDS
                )
        if tokens:
            console.print(
                f"[cyan]Generating word cloud from movie overviews ({suffix_label})..[/cyan]"
            )
            wordcloud = WordCloud(
                width=800, height=400, background_color="white"
            ).generate(" ".join(tokens))
            wordcloud_output_path = Path(output_directory) / f"wordcloud_{suffix_label}.png"
            wordcloud.to_file(wordcloud_output_path)
            console.print(
                f"[green]Word cloud from overviews ({suffix_label}) saved to {wordcloud_output_path}[/green]"
            )
        else:
            console.print(f"[yellow]No text for word cloud ({suffix_label})[/yellow]")

    generate_wordcloud(analysis_df["overview"], "overall")

    average_popularity_by_year = analysis_df.groupby("year")[
        "popularity"
    ].mean().sort_index()
    console.print("[cyan]Analyzing average movie popularity by year..[/cyan]")
    average_popularity_by_year.to_csv(Path(output_directory) / "avg_popularity_by_year.csv")
    console.print(
        f"[green]Report on average popularity by year saved to {Path(output_directory) / 'avg_popularity_by_year.csv'}[/green]"
    )

    top_twenty_westerns = analysis_df.nlargest(20, "popularity")[["title", "popularity", "year"]]
    console.print("[cyan]Identifying top 20 Westerns by popularity..[/cyan]")
    top_twenty_westerns.to_csv(
        Path(output_directory) / "top20_westerns_by_popularity.csv",
        index=False,
    )
    console.print(
        f"[green]Report on top 20 most popular Westerns saved to {Path(output_directory) / 'top20_westerns_by_popularity.csv'}[/green]"
    )

    console.print(
        f"[green]Analysis complete. Reports generated in '{output_directory}'[/green]"
    )


@app.command()
def analyze_descriptions(top_n: int = 30) -> None:
    console.rule("[bold cyan]Analyzing Movie Descriptions[/bold cyan]")
    with db_connect() as db_connection, db_connection.cursor() as db_cursor:
        db_cursor.execute("""
          SELECT origin_country[1], overview
          FROM movies
          WHERE overview IS NOT NULL
            AND array_length(origin_country,1)=1
            AND origin_country[1] IN ('US','IT')
        """
        )
        description_rows = db_cursor.fetchall()

    if not description_rows:
        console.print("[yellow]No descriptions found[/yellow]")
        return

    corpora_by_country = {"US": [], "IT": []}
    for origin_country_code, overview_text in description_rows:
        tokens = [
            word_token
            for word_token in nltk.word_tokenize(overview_text.lower())
            if word_token.isalpha() and word_token not in STOPWORDS
        ]
        corpora_by_country[origin_country_code].extend(tokens)

    for country_code, tokens in corpora_by_country.items():
        tokenFrequencies = Counter(tokens).most_common(top_n)
        result_table = Table(title=f"Top-{top_n} words ({country_code})")
        result_table.add_column("Rank")
        result_table.add_column("Word")
        result_table.add_column("Frequency")
        for rank_index, (word, count) in enumerate(tokenFrequencies, start=1):
            result_table.add_row(str(rank_index), word, str(count))
        console.print(result_table)
        

@app.command()
def execute_queries() -> None:
    console.rule("[bold cyan]Executing Database Queries[/bold cyan]")
    query_descriptions_and_sql = [
        (
            "1. Fetches the titles and release dates of movies released within the last year from the csorrent date. Results are ordered by release date in descending order and limited to the top 5.",
            "SELECT title, release_date FROM movies WHERE release_date > CURRENT_DATE - INTERVAL '1 year' ORDER BY release_date DESC LIMIT 5",
        ),
        (
            "2. Retrieves the titles and runtimes of movies classified as 'Western'. The results are ordered by runtime in descending order to show the longest Westerns first, limited to the top 5.",
            """SELECT m.title, m.runtime FROM movies m
             JOIN movie_genres mg ON m.id = mg.movie_id
             JOIN genres g ON g.id = mg.genre_id
             WHERE g.name = 'Western' AND m.runtime IS NOT NULL
             ORDER BY m.runtime DESC LIMIT 5""",
        ),
        (
            "3. Lists the titles, runtimes, and director names for films with a runtime greater than 150 minutes. It extracts the director's name from the 'crew' JSONB field, assuming the first crew member with the role 'director' is the one to display. Limited to 5 results.",
            """SELECT title, runtime, crew->0->'name' AS director_name FROM movies
             WHERE runtime > 150 AND crew @> '[{"role":"director"}]' LIMIT 5""",
        ),
        (
            "4. Calculates the total number of movies for each decade. It extracts the year from the release_date, groups by decade, and counts the movies, ordering the results by decade.",
            """SELECT (EXTRACT(YEAR FROM release_date)::int / 10) * 10 AS decade,
                   COUNT(*) AS total_movies
              FROM movies WHERE release_date IS NOT NULL
             GROUP BY decade
             ORDER BY decade""",
        ),
        (
            "5. Selects titles and runtimes of movies whose runtime is greater than the average runtime of all movies in the database (where runtime is not null). Results are ordered by runtime in descending order and limited to the top 5.",
            """SELECT title, runtime FROM movies
             WHERE runtime > (SELECT AVG(runtime) FROM movies WHERE runtime IS NOT NULL)
             ORDER BY runtime DESC LIMIT 5""",
        ),
        (
            "6. Shows the titles of Western movies along with their directors. It joins movies with genres to filter for 'Western' and extracts the director's name from the 'crew' JSONB field. Limited to 5 results.",
            """SELECT m.title, m.crew->0->'name' AS director_name
              FROM movies m
              JOIN movie_genres mg ON m.id = mg.movie_id
              JOIN genres g   ON g.id = mg.genre_id
             WHERE g.name = 'Western' AND m.crew @> '[{"role":"director"}]'
             LIMIT 5""",
        ),
        (
            "7. Finds Western movies directed by 'Clint Eastwood'. It filters by genre and checks the 'crew' JSONB field for a director named 'Clint Eastwood'. Results are ordered by release date descending, limited to 5.",
            """SELECT m.title, m.release_date FROM movies m
             JOIN movie_genres mg ON m.id = mg.movie_id
             JOIN genres g ON g.id = mg.genre_id
             WHERE g.name = 'Western' AND m.crew @> '[{"role":"director", "name":"Clint Eastwood"}]'
             ORDER BY m.release_date DESC LIMIT 5""",
        ),
        (
            "8. Counts the number of movies for each genre, but only includes genres that have more than 10 films. Results are ordered by the movie count in descending order.",
            """SELECT g.name AS genre, COUNT(m.id) AS movie_count FROM genres g
             JOIN movie_genres mg ON g.id = mg.genre_id
             JOIN movies m ON mg.movie_id = m.id
             GROUP BY g.name HAVING COUNT(m.id) > 10
             ORDER BY movie_count DESC""",
        ),
        (
            "9. Identifies directors who have directed more than one Western movie. It unnests the 'crew' JSONB array, filters for directors of Westerns, groups by director name, and counts their Westerns, showing only those with more than one. Results are ordered by the count of Westerns in descending order and limited to the top 5.",
            """SELECT director_name, COUNT(*) as western_count FROM (
              SELECT jsonb_array_elements(crew)->>'name' AS director_name
              FROM movies m
              JOIN movie_genres mg ON m.id = mg.movie_id
              JOIN genres g ON g.id = mg.genre_id
              WHERE g.name = 'Western' AND m.crew @> '[{"role":"director"}]' AND jsonb_array_length(m.crew) > 0
            ) AS directors
             WHERE director_name IS NOT NULL GROUP BY director_name HAVING COUNT(*) > 1 ORDER BY western_count DESC LIMIT 5""",
        ),
        (
            "10. Calculates the average runtime for movies in each genre, considering only films released on or after January 1, 2000, and having a non-null runtime. Results are ordered by average runtime in descending order and limited to the top 10 genres.",
            """SELECT g.name AS genre, AVG(m.runtime) AS average_runtime FROM genres g
             JOIN movie_genres mg ON g.id = mg.genre_id
             JOIN movies m ON mg.movie_id = m.id
             WHERE m.release_date >= '2000-01-01' AND m.runtime IS NOT NULL
             GROUP BY g.name ORDER BY average_runtime DESC LIMIT 10""",
        ),
        (
            "11. Retrieves titles, vote averages, and budgets for movies that have a vote average greater than 7.5 and a budget exceeding $1,000,000. Results are ordered first by vote average (descending) and then by budget (descending), limited to the top 5.",
            """SELECT title, vote_average, budget FROM movies
             WHERE vote_average > 7.5 AND budget > 1000000
             ORDER BY vote_average DESC, budget DESC LIMIT 5""",
        ),
    ]

    with db_connect() as db_connection, db_connection.cursor() as db_cursor:
        for description, sql_statement in query_descriptions_and_sql:
            console.print(f"[bold cyan]{description}[/bold cyan]")
            db_cursor.execute(sql_statement)
            query_results = db_cursor.fetchall()
            if not query_results:
                console.print("[yellow]No results[/yellow]")
                continue

            result_table = Table(show_header=True, header_style="bold magenta")
            if db_cursor.description:
                for column_info in db_cursor.description:
                    result_table.add_column(column_info[0])

            for result_row in query_results:
                result_table.add_row(*[str(cell) for cell in result_row])

            console.print(result_table)
            console.print("")


if __name__ == "__main__":
    app()
