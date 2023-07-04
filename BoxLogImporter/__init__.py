import os
import json
import datetime
import time
from boxsdk.auth.jwt_auth import JWTAuth
from boxsdk import Client
from boxsdk.object.events import Events, EnterpriseEventsStreamType
from .sentinel_connector import AzureSentinelConnector
from .state_manager import StateManager
from .log_query import LogQuery
from dateutil.parser import parse as parse_date
import logging
import re
from typing import Any, Tuple
import azure.functions as func


CLIENT_ID = os.environ['ClientId']
CLIENT_SECRET = os.environ['ClientSecret']
TENANT_ID = os.environ['TenantId']
WORKSPACE_ID = os.environ['AzureSentinelWorkspaceId']
SHARED_KEY = os.environ['AzureSentinelSharedKey']
logAnalyticsUri = os.environ.get('logAnalyticsUri')
LOG_TYPE = 'BoxEvents'
# 現在時刻よりも何分前のデータまで確認するか (既定: 120 分)
Delay_Minutes = os.environ.get('Delay_Minutes', 120)
Historical_Data_Days = os.environ.get('Historical_Data_Days', 3)
Delay_Minutes = int(Delay_Minutes)
Historical_Data_Days = int(Historical_Data_Days)
# 1 回の関数の実行で最大何分分のデータを取得するか (既定値: 60 分)
Max_Period_Minutes = os.environ.get('Max_Period_Minutes', 60)
Max_Period_Minutes = int(Max_Period_Minutes)
DryRun = os.environ.get('DryRun', 'True').lower() == 'true'
# 1 度に取得する最大の行数 (既定値: 200)
Max_Rows = int(os.environ.get('Max_Rows', 200))
# 詳細のログを出力するかどうか (既定値: True)
Verbose = os.environ.get('Verbose', 'True').lower() == 'true'

if ((logAnalyticsUri in (None, '') or str(logAnalyticsUri).isspace())):
    logAnalyticsUri = 'https://' + WORKSPACE_ID + '.ods.opinsights.azure.com'

pattern = r"https:\/\/([\w\-]+)\.ods\.opinsights\.azure.([a-zA-Z\.]+)$"
match = re.match(pattern, str(logAnalyticsUri))
if not match:
    raise Exception("Invalid Log Analytics Uri.")

# interval of script execution
SCRIPT_EXECUTION_INTERVAL_MINUTES = int(os.environ.get('Script_Execution_Internal_Minutes', 10))
# max azure function lifetime
AZURE_FUNC_MAX_EXECUTION_TIME_MINUTES = int(os.environ.get('Azure_Func_Max_Execution_Time_Minutes', 6))


def main(mytimer: func.TimerRequest) -> None:
    start_time = time.time()
    current_time = start_time
    logging.getLogger().setLevel(logging.INFO)
    config_json = os.environ['BOX_CONFIG_JSON']
    config_dict = json.loads(config_json)
    while current_time - start_time < AZURE_FUNC_MAX_EXECUTION_TIME_MINUTES * 60:
        result = process(config_dict)
        if not result:
            break
        current_time = time.time()


def process(config_dict: Any) -> bool:
    logging.getLogger().setLevel(logging.INFO)
    logging.info(f'Parameters initialized are  WORKSPACE_ID: {WORKSPACE_ID} SHARED_KEY: ItsASecret logAnalyticsUri: {logAnalyticsUri} LOG_TYPE: {LOG_TYPE}  Delay_Minutes: {Delay_Minutes} Historical_Data_Days: {Historical_Data_Days} SCRIPT_EXECUTION_INTERVAL_MINUTES: {SCRIPT_EXECUTION_INTERVAL_MINUTES} AZURE_FUNC_MAX_EXECUTION_TIME_MINUTES: {AZURE_FUNC_MAX_EXECUTION_TIME_MINUTES} Max_Period_Minutes: {Max_Period_Minutes} ')

    created_before = datetime.datetime.utcnow() - datetime.timedelta(minutes=Delay_Minutes)
    created_before = created_before.replace(tzinfo=datetime.timezone.utc, second=0, microsecond=0)

    file_storage_connection_string = os.environ['AzureWebJobsStorage']
    state_manager = StateManager(connection_string=file_storage_connection_string)

    # ストレージ アカウントに保存された、前回の取得位置を取得
    stream_position, created_after = get_stream_pos_and_date_from(
        marker=state_manager.get()
    )

    created_after = created_after + datetime.timedelta(seconds=1)

    if created_after > created_before:
        return False

    if created_after + datetime.timedelta(minutes=Max_Period_Minutes) < created_before:
        created_before = created_after + datetime.timedelta(minutes=Max_Period_Minutes)
        logging.info('Backlog to process is more than than {} minutes. So changing created_before to {}. Remaining data will be processed during next invocation'.format(Max_Period_Minutes, created_before))

    logging.info('Script started. Getting events from created_before {}, created_after {}'.format(created_before, created_after))

    log_query = LogQuery(CLIENT_ID, CLIENT_SECRET, TENANT_ID, WORKSPACE_ID)
    query = 'BoxEvents_CL | where created_at_t between(datetime(\'{}\')..datetime(\'{}\')) | project TimeGenerated, created_at_t, event_id_g'.format(created_after, created_before)
    results = log_query.query(query)

    if Verbose:
        logging.info('Log Analytics query result: rows={}'.format(len(results)))
        if len(results) > 0:
            logging.info('first event: event_id={}, created_at={}, TimeGenerated={}'.format(results[0][2], results[0][1], results[0][0]))
            logging.info('last event: event_id={}, created_at={}, TimeGenerated={}'.format(results[-1][2], results[-1][1], results[-1][0]))

    sentinel = AzureSentinelConnector(workspace_id=WORKSPACE_ID, logAnalyticsUri=logAnalyticsUri, shared_key=SHARED_KEY, log_type=LOG_TYPE, queue_size=10000)
    with sentinel:
        reservoir = []
        reservoir_match_count = 0
        last_event_date = None
        next_stream_position = 0
        skipped = False
        for i, (events, next_stream_position) in enumerate(get_events(config_dict, created_after=created_after, created_before=created_before, stream_position=stream_position)):
            missing_ids = []
            for event in events:
                found = False
                event_date = datetime.datetime.fromisoformat(event['created_at'])
                if event_date > created_before:
                    logging.info('skipped event_id: {}, created_at: {} > created_before: {}'.format(event['event_id'], event['created_at'], created_before))
                    skipped = True
                    continue
                elif event_date < created_after:
                    logging.info('skipped event_id: {}, created_at: {} < created_after'.format(event['event_id'], event['created_at'], created_after))
                    skipped = True
                for j, row in enumerate(results):
                    if row[2] == event['event_id']:
                        found = True
                        if j != 0:
                            logging.info('copy {} rows to reservoir'.format(j))
                            reservoir += results[:j]
                        results = results[j + 1:]
                        break
                for row in reservoir:
                    if row[2] == event['event_id']:
                        found = True
                        reservoir_match_count += 1
                        break
                if not found:
                    missing_ids.append(event['event_id'])
                    logging.info('event_id: {}, created_at: {} not found.'.format(event['event_id'], event['created_at']))
                    if not DryRun:
                        sentinel.send(event)

            logging.getLogger().setLevel(logging.INFO)
            last_event_date = events[-1]['created_at'] if events else last_event_date
            logging.info('[{}] Processed {} events. next_stream_position: {}, Last event date: {}. {} events are not found in LA.'.format(i, len(events), next_stream_position, last_event_date, len(missing_ids)))

        if Verbose:
            logging.info('reservoir length: {}, reservoir_match_count: {}'.format(len(reservoir), reservoir_match_count))

        if skipped:
            save_marker(state_manager, 0, str(created_before))
        elif last_event_date:
            save_marker(state_manager, next_stream_position, last_event_date)
        elif created_before:
            save_marker(state_manager, next_stream_position, str(created_before))

    return True


def get_stream_pos_and_date_from(marker: str) -> Tuple[int, datetime.datetime]:
    def get_default_date_from() -> datetime.datetime:
        date_from = datetime.datetime.utcnow() - datetime.timedelta(minutes=Historical_Data_Days * 24 * 60)
        date_from = date_from.replace(tzinfo=datetime.timezone.utc, second=0, microsecond=0)
        return date_from

    def get_token_from_maker(marker: str) -> Tuple[str, datetime.datetime]:
        # ストレージアカウントの保存されたトークンと日付を取得
        token = 0
        last_event_date = None
        try:
            token, last_event_date = marker.split(' ', 1)
            last_event_date = parse_date(last_event_date).astimezone(datetime.timezone.utc)
        except Exception:
            pass
        return token, last_event_date

    token, last_event_date = get_token_from_maker(marker)
    if last_event_date:
        date_from = last_event_date
    else:
        date_from = get_default_date_from()

    return int(token), date_from


def save_marker(state_manager: StateManager, stream_position: int, last_event_date: str) -> None:
    logging.getLogger().setLevel(logging.INFO)
    logging.info('Saving last stream_position {} and last_event_date {}'.format(stream_position, last_event_date))
    state_manager.post(str(stream_position) + ' ' + last_event_date)

def get_events(config_dict, created_after=None, created_before=None, stream_position=0):
    logging.getLogger().setLevel(logging.WARNING)
    limit = Max_Rows
    config = JWTAuth.from_settings_dictionary(config_dict)
    client = Client(config)
    events_client = client.events()

    while True:
        res = events_client.get_admin_events(stream_position=stream_position, created_after=created_after, created_before=created_before, limit=limit)
        stream_position = res['next_stream_position']
        events = [event.response_object for event in res['entries']]
        if len(events) < limit:
            stream_position = 0
        yield events, stream_position
        if stream_position == 0:
            break
