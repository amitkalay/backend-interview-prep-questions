"""
Database Utility Class for Movie and Actor Database
Parses SQL files and creates/populates an SQLite database
"""

import sqlite3
import re
from typing import List, Dict, Any, Tuple
from pathlib import Path


class MovieDatabaseUtility:
    """
    A utility class to manage and query movie and actor data.
    
    This class:
    1. Parses actors.sql and movies.sql files
    2. Creates SQLite tables (movies and actors)
    3. Inserts data into the database
    4. Provides query methods for:
       - Longest-running movie
       - Movie with the most actors
       - Breakdown of top movies by rating
    """

    def __init__(self, sql_dir: str, db_file: str = "database_raw_file"):
        """
        Initialize the database utility.
        
        Args:
            sql_dir: Directory containing actors.sql and movies.sql
            db_file: Name of the SQLite database file to create/use
        """
        self.sql_dir = Path(sql_dir)
        self.db_file = Path(sql_dir) / db_file

        # Initialize database (create file and tables)
        self._initialize_database()

        # Parse and load data
        self._parse_and_insert_sql_files()

    def _initialize_database(self) -> None:
        """Initialize SQLite database connection and create tables."""
        # Use a context manager to ensure commit and close per operation
        with sqlite3.connect(str(self.db_file)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Drop existing tables if they exist (for fresh initialization)
            cursor.execute("DROP TABLE IF EXISTS actors")
            cursor.execute("DROP TABLE IF EXISTS movies")

            # Create movies table
            cursor.execute("""
            CREATE TABLE movies (
                id INTEGER PRIMARY KEY NOT NULL,
                imdb_id VARCHAR(20) NOT NULL,
                title VARCHAR(255) NOT NULL,
                director VARCHAR(255) NOT NULL,
                year INTEGER NOT NULL,
                rating VARCHAR(8) NOT NULL,
                genres VARCHAR(255) NOT NULL,
                runtime INTEGER NOT NULL,
                country VARCHAR(255) NOT NULL,
                language VARCHAR(255) NOT NULL,
                imdb_score REAL NOT NULL,
                imdb_votes INTEGER NOT NULL,
                metacritic_score REAL NOT NULL
            )
        """)
        
            # Create actors table
            cursor.execute("""
            CREATE TABLE actors (
                id INTEGER PRIMARY KEY NOT NULL,
                movie_id INTEGER NOT NULL,
                imdb_id VARCHAR(20) NOT NULL,
                name VARCHAR(255) NOT NULL,
                FOREIGN KEY (movie_id) REFERENCES movies(id)
            )
        """)
        print("✓ Database tables created successfully")

    def _parse_and_insert_sql_files(self) -> None:
        """Parse actors.sql and movies.sql files and insert data into database."""
        actors_file = self.sql_dir / "actors.sql"
        movies_file = self.sql_dir / "movies.sql"
        
        if not actors_file.exists():
            raise FileNotFoundError(f"actors.sql not found in {self.sql_dir}")
        if not movies_file.exists():
            raise FileNotFoundError(f"movies.sql not found in {self.sql_dir}")
        
        # Insert movies first (since actors reference movies via foreign key)
        self._insert_movies(movies_file)
        # Then insert actors
        self._insert_actors(actors_file)

        print("✓ Data inserted successfully into database")

    def _insert_actors(self, file_path: Path) -> None:
        """Parse actors.sql file and insert into database."""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Pattern to extract INSERT statements
        pattern = r"INSERT INTO actors VALUES\((\d+),(\d+),'([^']+)','([^']+)'\)"
        matches = re.finditer(pattern, content)
        
        inserted_count = 0

        # Use a context manager so each insert batch is committed and connection closed
        with sqlite3.connect(str(self.db_file)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            for match in matches:
                actor_id, movie_id, imdb_id, name = match.groups()
                try:
                    cursor.execute("""
                        INSERT INTO actors (id, movie_id, imdb_id, name)
                        VALUES (?, ?, ?, ?)
                    """, (int(actor_id), int(movie_id), imdb_id, name))
                    inserted_count += 1
                except sqlite3.IntegrityError as e:
                    print(f"Warning: Could not insert actor {actor_id}: {e}")

        print(f"  • Inserted {inserted_count} actors")

    def _insert_movies(self, file_path: Path) -> None:
        """Parse movies.sql file and insert into database."""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Pattern to extract INSERT statements for movies
        pattern = r"INSERT INTO movies VALUES\((\d+),'([^']+)','([^']*?)','([^']*?)'\s*,(\d+),'([^']*?)','([^']*?)'\s*,(\d+),'([^']*?)','([^']*?)'\s*,([0-9.]+),(\d+),([0-9.]+)\)"
        matches = re.finditer(pattern, content)
        
        inserted_count = 0

        with sqlite3.connect(str(self.db_file)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            for match in matches:
                groups = match.groups()
                movie_id = int(groups[0])

                try:
                    cursor.execute("""
                        INSERT INTO movies (
                            id, imdb_id, title, director, year, rating,
                            genres, runtime, country, language,
                            imdb_score, imdb_votes, metacritic_score
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        movie_id,
                        groups[1],                    # imdb_id
                        groups[2],                    # title
                        groups[3],                    # director
                        int(groups[4]),              # year
                        groups[5],                    # rating
                        groups[6],                    # genres
                        int(groups[7]),              # runtime
                        groups[8],                    # country
                        groups[9],                    # language
                        float(groups[10]),           # imdb_score
                        int(groups[11]),             # imdb_votes
                        float(groups[12])            # metacritic_score
                    ))
                    inserted_count += 1
                except sqlite3.IntegrityError as e:
                    print(f"Warning: Could not insert movie {movie_id}: {e}")

        print(f"  • Inserted {inserted_count} movies")

    def get_longest_running_movie(self) -> Dict[str, Any]:
        """
        Query: What is the longest-running movie?
        
        Returns:
            Dictionary containing the longest-running movie details
        """
        with sqlite3.connect(str(self.db_file)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    m.*,
                    COUNT(a.id) as actor_count
                FROM movies m
                LEFT JOIN actors a ON m.id = a.movie_id
                GROUP BY m.id
                ORDER BY m.runtime DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
        if not result:
            return {}
        
        return {
            'title': result['title'],
            'runtime': result['runtime'],
            'year': result['year'],
            'director': result['director'],
            'imdb_score': result['imdb_score'],
            'actor_count': result['actor_count'],
            'full_details': dict(result)
        }

    def get_movie_with_most_actors(self) -> Dict[str, Any]:
        """
        Query: What movie has the most actors?
        
        Returns:
            Dictionary containing the movie with most actors and actor list
        """
        with sqlite3.connect(str(self.db_file)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    m.*,
                    COUNT(a.id) as actor_count
                FROM movies m
                LEFT JOIN actors a ON m.id = a.movie_id
                GROUP BY m.id
                ORDER BY actor_count DESC
                LIMIT 1
            """)

            result = cursor.fetchone()
            if not result:
                return {}

            # Get all actors for this movie
            cursor.execute("""
                SELECT id, imdb_id, name
                FROM actors
                WHERE movie_id = ?
                ORDER BY name
            """, (result['id'],))

            actors = [dict(row) for row in cursor.fetchall()]
        
        return {
            'title': result['title'],
            'actor_count': result['actor_count'],
            'year': result['year'],
            'director': result['director'],
            'imdb_score': result['imdb_score'],
            'actors': actors,
            'full_details': dict(result)
        }

    def get_top_movies_by_rating_breakdown(self, top_n: int = 10) -> Dict[str, Any]:
        """
        Query: What is the breakdown of top movies by rating?
        
        Returns top N movies sorted by IMDB score with breakdowns.
        
        Args:
            top_n: Number of top movies to include (default: 10)
        
        Returns:
            Dictionary containing top movies breakdown by rating
        """
        with sqlite3.connect(str(self.db_file)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get top N movies by IMDB score
            cursor.execute("""
                SELECT 
                    m.*,
                    COUNT(a.id) as actors_count
                FROM movies m
                LEFT JOIN actors a ON m.id = a.movie_id
                GROUP BY m.id
                ORDER BY m.imdb_score DESC
                LIMIT ?
            """, (top_n,))

            top_movies = [dict(row) for row in cursor.fetchall()]
        
        if not top_movies:
            return {}
        
        # Group by rating category
        rating_breakdown = {}
        rating_distribution = {}
        
        for movie in top_movies:
            rating = movie['rating']
            if rating not in rating_breakdown:
                rating_breakdown[rating] = []
                rating_distribution[rating] = 0
            
            rating_breakdown[rating].append({
                'title': movie['title'],
                'imdb_score': movie['imdb_score'],
                'year': movie['year'],
                'director': movie['director'],
                'actors_count': movie['actors_count']
            })
            rating_distribution[rating] += 1
        
        return {
            'total_movies_in_top': len(top_movies),
            'rating_categories': rating_breakdown,
            'rating_distribution': rating_distribution
        }

    def get_all_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the database.
        
        Returns:
            Dictionary with various statistics
        """
        with sqlite3.connect(str(self.db_file)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get total counts
            cursor.execute("SELECT COUNT(*) as total FROM movies")
            total_movies = cursor.fetchone()['total']

            cursor.execute("SELECT COUNT(*) as total FROM actors")
            total_actors = cursor.fetchone()['total']

            # Get average runtime and score
            cursor.execute("""
                SELECT 
                    AVG(runtime) as avg_runtime,
                    AVG(imdb_score) as avg_score
                FROM movies
            """)
            avg_data = cursor.fetchone()

            # Get year range
            cursor.execute("""
                SELECT 
                    MIN(year) as min_year,
                    MAX(year) as max_year
                FROM movies
            """)
            year_data = cursor.fetchone()

            # Get rating distribution
            cursor.execute("""
                SELECT rating, COUNT(*) as count
                FROM movies
                GROUP BY rating
                ORDER BY rating
            """)
            rating_dist = {row['rating']: row['count'] for row in cursor.fetchall()}
        
        return {
            'total_movies': total_movies,
            'total_actors': total_actors,
            'average_runtime': round(avg_data['avg_runtime'], 2) if avg_data['avg_runtime'] else 0,
            'average_imdb_score': round(avg_data['avg_score'], 2) if avg_data['avg_score'] else 0,
            'rating_distribution': rating_dist,
            'year_range': {
                'min': year_data['min_year'],
                'max': year_data['max_year']
            }
        }

    def query_movies_by_director(self, director: str) -> List[Dict[str, Any]]:
        """
        Query movies by director name.
        
        Args:
            director: Director name (partial match supported)
        
        Returns:
            List of movies matching the director
        """
        with sqlite3.connect(str(self.db_file)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    m.*,
                    COUNT(a.id) as actors_count
                FROM movies m
                LEFT JOIN actors a ON m.id = a.movie_id
                WHERE m.director LIKE ?
                GROUP BY m.id
                ORDER BY m.imdb_score DESC
            """, (f"%{director}%",))

            return [dict(row) for row in cursor.fetchall()]

    def query_movies_by_rating(self, rating: str) -> List[Dict[str, Any]]:
        """
        Query movies by rating category.
        
        Args:
            rating: Rating category (e.g., 'R', 'PG-13', 'G')
        
        Returns:
            List of movies with specified rating
        """
        with sqlite3.connect(str(self.db_file)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    m.*,
                    COUNT(a.id) as actors_count
                FROM movies m
                LEFT JOIN actors a ON m.id = a.movie_id
                WHERE m.rating = ?
                GROUP BY m.id
                ORDER BY m.imdb_score DESC
            """, (rating,))

            return [dict(row) for row in cursor.fetchall()]


def main():
    """Main function to demonstrate usage of the DatabaseUtility class."""
    # Initialize the database utility
    db = MovieDatabaseUtility(
        sql_dir="/Users/amitkalay/Desktop/backend-interview-prep-questions",
        db_file="database_raw_file"
    )
    
    print("\n" + "=" * 80)
    print("MOVIE DATABASE UTILITY - QUERY RESULTS")
    print("=" * 80)
    
    # Query 1: Longest-running movie
    print("\n1. LONGEST-RUNNING MOVIE:")
    print("-" * 80)
    longest = db.get_longest_running_movie()
    print(f"   Title: {longest['title']}")
    print(f"   Runtime: {longest['runtime']} minutes")
    print(f"   Year: {longest['year']}")
    print(f"   Director: {longest['director']}")
    print(f"   IMDB Score: {longest['imdb_score']}")
    print(f"   Number of Actors: {longest['actor_count']}")
    
    # Query 2: Movie with most actors
    print("\n2. MOVIE WITH THE MOST ACTORS:")
    print("-" * 80)
    most_actors = db.get_movie_with_most_actors()
    print(f"   Title: {most_actors['title']}")
    print(f"   Number of Actors: {most_actors['actor_count']}")
    print(f"   Year: {most_actors['year']}")
    print(f"   Director: {most_actors['director']}")
    print(f"   IMDB Score: {most_actors['imdb_score']}")
    print(f"\n   Top 10 Actors:")
    for i, actor in enumerate(most_actors['actors'][:10], 1):
        print(f"   {i}. {actor['name']}")
    
    # Query 3: Top movies by rating breakdown
    print("\n3. TOP 10 MOVIES BY RATING - BREAKDOWN:")
    print("-" * 80)
    rating_breakdown = db.get_top_movies_by_rating_breakdown(top_n=10)
    print(f"\n   Total Movies in Top 10: {rating_breakdown['total_movies_in_top']}")
    print(f"\n   Rating Distribution:")
    for rating, count in rating_breakdown['rating_distribution'].items():
        print(f"   - {rating}: {count} movie(s)")
    
    print(f"\n   Movies by Rating Category:")
    for rating, movies in rating_breakdown['rating_categories'].items():
        print(f"\n   {rating}:")
        for movie in movies:
            print(f"      • {movie['title']} ({movie['year']}) - "
                  f"Score: {movie['imdb_score']}, Actors: {movie['actors_count']}")
    
    # General Statistics
    print("\n4. DATABASE STATISTICS:")
    print("-" * 80)
    stats = db.get_all_stats()
    print(f"   Total Movies: {stats['total_movies']}")
    print(f"   Total Actors: {stats['total_actors']}")
    print(f"   Average Runtime: {stats['average_runtime']} minutes")
    print(f"   Average IMDB Score: {stats['average_imdb_score']}")
    print(f"   Year Range: {stats['year_range']['min']} - {stats['year_range']['max']}")
    print(f"\n   Rating Distribution:")
    for rating, count in sorted(stats['rating_distribution'].items()):
        print(f"   - {rating}: {count} movie(s)")
    
    print("\n" + "=" * 80)
    print(f"SQLite Database created: database_raw_file")
    print("=" * 80)


if __name__ == "__main__":
    main()
