import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import date
from scipy.stats import poisson
from api_service.config import settings

DB_DSN = settings.DATABASE_URL
LEAGUE_AVG_TOTAL = 8.5

def get_game_totals() -> pd.DataFrame:
    conn = psycopg2.connect(DB_DSN)
    df = pd.read_sql(
        "SELECT game_id, side, book, odds "
        "FROM game_totals WHERE timestamp::date = %s",
        conn,
        params=[date.today()]
    )
    conn.close()
    return df

def poisson_prob_totals(side: str) -> float:
    # Extract numeric threshold, e.g. '8.5'
    thresh = float(side.split()[1])
    lam    = LEAGUE_AVG_TOTAL
    if side.lower().startswith("over"):
        # P(X >= thresh) = 1 - P(X <= thresh-1)
        return 1 - poisson.cdf(int(thresh), lam)
    else:
        # P(X <= thresh)
        return poisson.cdf(int(thresh), lam)

def compute_totals_value(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    df['model_p'] = df['side'].apply(poisson_prob_totals)
    df['implied_p'] = 1 / df['odds']
    df['value'] = df['model_p'] - df['implied_p']

    #pivot so each book's odds become their own column
    pivot = df.pivot_table(
        index=['game_id', 'side'],
        columns='book',
        values='odds'
    ).reset_index()

    #find bets book
    best = df.loc[
        df.groupby(['game_id', 'side'])['value'].idxmax(),
        ['game_id', 'side', 'book', 'model_p', 'implied_p', 'value']
    ].rename(columns={'book': 'best_book'})

    # Merge pivoted odds with best_book info
    return pivot.merge(best, on=['game_id', 'side'])

def persist_value_totals(df: pd.DataFrame):
    rows = [
        (
            r.game_id,
            r.side,
            r.model_p,
            r.implied_p,
            r.value,
            r.best_book
        )
        for _, r in df.iterrows()
    ]
    cols = ['game_id','side','model_p','implied_p','value','best_book']
    set_clause = ",".join(f"{c}=EXCLUDED.{c}" for c in cols[1:]) + ", updated_at=NOW()"
    sql = f"""
      INSERT INTO value_totals ({','.join(cols)})
      VALUES %s
      ON CONFLICT (game_id) DO UPDATE SET {set_clause};
    """
    conn = psycopg2.connect(DB_DSN)
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    conn.close()
