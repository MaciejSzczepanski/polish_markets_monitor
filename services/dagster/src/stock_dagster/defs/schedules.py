import dagster as dg

from stock_dagster.defs.jobs import currency_daily, currency_unpopular, news_update, wig20_metadata

news = dg.ScheduleDefinition(
    description="""Today's news articles from selected sources""",
    job=news_update,
    cron_schedule="5 7-23 * * *",
    execution_timezone='Europe/Warsaw'
)

currency = dg.ScheduleDefinition(
    description="Today's currency rates for popular currencies, mid market rate and bid/ask",
    job=currency_daily,
    cron_schedule='0 12 * * *',
    execution_timezone='Europe/Warsaw'
)

currency_unpopular = dg.ScheduleDefinition(
    description="Update unpopular currencies rates",
    job=currency_unpopular,
    cron_schedule='55 12 * * 3',
    execution_timezone='Europe/Warsaw'
)

wig20_metadata = dg.ScheduleDefinition(
    description="WIG20 Companies metadata update",
    job=wig20_metadata,
    cron_schedule="0 12 15 * *",
    execution_timezone='Europe/Warsaw'
)
