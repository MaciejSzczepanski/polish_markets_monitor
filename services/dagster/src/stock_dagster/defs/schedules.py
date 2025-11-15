import dagster as dg

from stock_dagster.defs.jobs import currency_daily, currency_unpopular, news_update, wig20_metadata, ohlc_job, \
    gemini_job, gold_job

news = dg.ScheduleDefinition(
    description="""Today's news articles from selected sources""",
    job=news_update,
    cron_schedule="5 7-23 * * *",
    execution_timezone='Europe/Warsaw'
)

currency = dg.ScheduleDefinition(
    description="Today's currency rates for popular currencies, mid market rate and bid/ask",
    job=currency_daily,
    cron_schedule='0 12 * * 1-5',
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

ohlc = dg.ScheduleDefinition(
    description="OHLC data for WIG20 companies",
    job=ohlc_job,
    cron_schedule="*/15 9-18 * * 1-5",
    execution_timezone='Europe/Warsaw'
)

gemini = dg.ScheduleDefinition(
    description="Summarize day by Gemini model",
    job=gemini_job,
    cron_schedule="30 17 * * 1-5",
    execution_timezone='Europe/Warsaw'
)

gold = dg.ScheduleDefinition(
    description="Gold rates",
    job=gold_job,
    cron_schedule='* 13 * * 1-5'
)