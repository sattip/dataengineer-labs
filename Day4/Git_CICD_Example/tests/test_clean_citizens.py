"""Unit tests για clean_citizen_data function.

Καλύπτει 3 σενάρια:
- Happy path: καθαρές εγγραφές περνούν
- Duplicates: διπλότυπα αφαιρούνται
- Invalid: null ΑΦΜ + λάθος email αφαιρούνται
"""
from pyspark.sql import SparkSession

from notebooks.clean_citizens import clean_citizen_data


def get_spark():
    """Δημιουργεί lightweight local Spark για tests."""
    return (SparkSession.builder
            .master('local[1]')
            .appName('test')
            .config('spark.sql.shuffle.partitions', '1')
            .getOrCreate())


def test_dedupe_and_filter():
    """Επικυρώνει ότι duplicates, null ΑΦΜ και λάθος email αφαιρούνται."""
    spark = get_spark()
    data = [
        (1, 'a@x'),    # valid
        (1, 'a@x'),    # duplicate → πρέπει να αφαιρεθεί
        (2, None),     # null email → πρέπει να αφαιρεθεί
        (3, 'no-at'),  # email χωρίς @ → πρέπει να αφαιρεθεί
        (None, 'b@y'), # null ΑΦΜ → πρέπει να αφαιρεθεί
    ]
    df = spark.createDataFrame(data, ['id', 'email'])

    result = clean_citizen_data(df).collect()

    assert len(result) == 1, f"Αναμένουμε 1 εγγραφή, βρήκαμε {len(result)}"
    assert result[0]['id'] == 1
    assert result[0]['email'] == 'a@x'


def test_empty_input():
    """Edge case: κενό DataFrame δίνει κενό αποτέλεσμα."""
    spark = get_spark()
    df = spark.createDataFrame([], 'id INT, email STRING')
    result = clean_citizen_data(df).collect()
    assert len(result) == 0
