import pandas as pd

def create_date_dim(start_date, end_date) -> pd.DataFrame:
    """
    Params: start_date, end_date
    Return: DataFrame (contains date)
    """
    # Create a DataFrame for the Date Dimension
    date_dim = pd.DataFrame({
        'date': pd.date_range(start=start_date, end=end_date)
    })

    # Add additional columns for the date dimension
    date_dim['day'] = date_dim['date'].dt.day
    date_dim['month'] = date_dim['date'].dt.month
    date_dim['year'] = date_dim['date'].dt.year
    date_dim['quarter'] = date_dim['date'].dt.quarter
    date_dim['day_of_week'] = date_dim['date'].dt.dayofweek  # Monday = 0, Sunday = 6
    date_dim['day_name'] = date_dim['date'].dt.day_name()
    date_dim['month_name'] = date_dim['date'].dt.month_name()
    date_dim['is_weekend'] = date_dim['day_of_week'].isin([5, 6]).astype(int)  # 1 for weekend, 0 for weekday
    date_dim['week_of_year'] = date_dim['date'].dt.isocalendar().week
    date_dim['is_holiday'] = 0  # You can customize this by adding a list of holidays

    # Add surrogate key
    date_dim['date_key'] = date_dim['date'].dt.strftime('%Y%m%d').astype(int)
    date_dim['date'] = date_dim['date'].dt.strftime("%Y-%m-%d")

    # Reorder columns
    date_dim = date_dim[['date_key', 'date', 'day', 'month', 'year', 'quarter', 'day_of_week', 'day_name', 'month_name', 'is_weekend', 'week_of_year', 'is_holiday']]
    return date_dim