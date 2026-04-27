"""Παράδειγμα data cleaning function για ΑΑΔΕ φορολογικές δηλώσεις.

Καθαρίζει εγγραφές πολιτών:
- Αφαιρεί διπλότυπα
- Φιλτράρει null ΑΦΜ
- Επικυρώνει format email (πρέπει να περιέχει @)
"""
from pyspark.sql.functions import col


def clean_citizen_data(df):
    """Returns a cleaned DataFrame with valid citizen records only.

    Args:
        df: Spark DataFrame με στήλες 'id' (ΑΦΜ) και 'email'.

    Returns:
        DataFrame χωρίς duplicates, null ΑΦΜ, ή λάθος email format.
    """
    return (df.dropDuplicates()
              .filter(col('id').isNotNull())
              .filter(col('email').rlike('@')))
