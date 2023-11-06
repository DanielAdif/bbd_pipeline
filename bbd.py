import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy import text
import urllib
import pyodbc
from datetime import datetime, time
from calendar import month_name
from dbhelper import DBHelperBBD, DBHelperBBD2
from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
    ExperimentalWarning
)

import warnings
warnings.filterwarnings("ignore", category=ExperimentalWarning)

db = DBHelperBBD()
db2 = DBHelperBBD2()

@asset
def get_time():
    current_time = datetime.now().time()
    if current_time >= time(7, 0) and current_time <= time(15, 40):
        shift = 'S1'
    elif current_time > time(15, 40) and current_time <= time(22, 45):
        shift = 'S2'
    elif current_time > time(22, 45):
        shift = 'S3'
    else:
        shift = None

    datecomplete    = datetime.now().strftime("%m/%d/%Y")
    return shift, datecomplete

@asset
def oee_BBD_Insert(get_time):
    shift           = get_time[0]
    datecomplete    = get_time[1]

    # print(shift)
    # print(datecomplete)

    length1 = len(db.get_oeeBBD(datecomplete, shift)) 
    # print(length1)
    oee_BBD = db.get_oeeBBD(datecomplete, shift)
    # print(oee_BBD)

    for x in range(length1):
        try:
            db2.insert_oee_BBD(
                oeeBBD_id = oee_BBD[x][0],
                oeeBBD_runtime = oee_BBD[x][1],
                oeeBBD_downtime = oee_BBD[x][2],
                oeeBBD_uptime = oee_BBD[x][3],
                oeeBBD_stdCT = oee_BBD[x][4],
                oeeBBD_actCT = oee_BBD[x][5],
                oeeBBD_planoutput = oee_BBD[x][6],
                oeeBBD_good = oee_BBD[x][7],
                oeeBBD_reject = oee_BBD[x][8],
                oeeBBD_output = oee_BBD[x][9],
                oeeBBD_availability = oee_BBD[x][10],
                oeeBBD_performance = oee_BBD[x][11],
                oeeBBD_quality = oee_BBD[x][12],
                oeeBBD_oee = oee_BBD[x][13],
                oeeBBD_utilization = oee_BBD[x][14]
            )
        except:
            pass

@asset
def dt_BBD_Insert(get_time):
    shift           = get_time[0]
    datecomplete    = get_time[1]

    # print(shift)
    # print(datecomplete)

    length1 = len(db.get_dt_BBD(datecomplete, shift)) 
    # print(length1)
    dt_BBD = db.get_dt_BBD(datecomplete, shift)
    # print(dt_BBD)

    for x in range(length1):
        try:
            db2.insert_dt_BBD(
                dtBBD_id = dt_BBD[x][0],
                dtBBD_toolproblem = dt_BBD[x][1],
                dtBBD_paintproblem =  dt_BBD[x][2],
                dtBBD_nomaterial= dt_BBD[x][3],
                dtBBD_finishschedule = dt_BBD[x][4],
                dtBBD_noproduction = dt_BBD[x][5],
                dtBBD_emergency = dt_BBD[x][6],
                dtBBD_preventivemaintenance = dt_BBD[x][7],
                dtBBD_changeover = dt_BBD[x][8],
                dtBBD_refreshpaint = dt_BBD[x][9],
                dtBBD_changeovermask = dt_BBD[x][10],
                dtBBD_total = dt_BBD[x][11]
            )
        except:
            pass

@asset
def dtlog_BBD_Insert(get_time):
    shift           = get_time[0]
    datecomplete    = get_time[1]

    # print(shift)
    # print(datecomplete)

    length1 = len(db.get_dtlog_BBD(datecomplete, shift)) 
    # print(length1)
    dtlog_BBD = db.get_dtlog_BBD(datecomplete, shift)
    # print(dtlog_BBD)

    for x in range(length1):
        try:
            db2.insert_dtlog_BBD(
                dtlogBBD_id = dtlog_BBD[x][0],
                dtlogBBD_dtstatus = dtlog_BBD[x][1]
            )
        except:
            pass

@asset
def qc_BBD_Insert(get_time):
    shift           = get_time[0]
    datecomplete    = get_time[1]

    print(shift)
    print(datecomplete)

    length1 = len(db.get_qcBBD(datecomplete, shift)) 
    print(length1)
    qc_BBD = db.get_qcBBD(datecomplete, shift)
    # print(qc_BBD)

    for x in range(length1):
        try:
            db2.insert_qc_BBD(
                qcBBD_id = qc_BBD[x][0],
                qcBBD_good = qc_BBD[x][1],
                qcBBD_reject = qc_BBD[x][2],
                qcBBD_total = qc_BBD[x][3],
                qcBBD_asymetricaleye = qc_BBD[x][4],
                qcBBD_mispositionpaintltcc = qc_BBD[x][5],
                qcBBD_scratchpaint = qc_BBD[x][6],
                qcBBD_peeloff = qc_BBD[x][7],
                qcBBD_contaminationpaint = qc_BBD[x][8],
                qcBBD_wrongcolor = qc_BBD[x][9],
                qcBBD_missingpaint = qc_BBD[x][10],
                qcBBD_dirty = qc_BBD[x][11],
                qcBBD_maskmark = qc_BBD[x][12],
                qcBBD_blackspot = qc_BBD[x][13],
                qcBBD_bubble = qc_BBD[x][14],
                qcBBD_deform = qc_BBD[x][15],
                qcBBD_flesh = qc_BBD[x][16],
            )
        except:
            pass


@asset
def oee_trend_BBD_Insert(get_time):
    datecomplete    = get_time[1]

    print(datecomplete)

    length1 = len(db.get_oeeTrendBBD(datecomplete)) 
    print(length1)
    oee_trend_BBD = db.get_oeeTrendBBD(datecomplete)
    print(oee_trend_BBD)

    for x in range(length1):
        try:
            db2.insert_oee_trend_BBD(
                oee_trend_1 = oee_trend_BBD[x][0],
                oee_trend_2 = oee_trend_BBD[x][1]
            )
        except:
            pass


update_bbd = define_asset_job(name="update_bbd", selection=AssetSelection.all())

bbd1 = ScheduleDefinition(
    name="bbd1", job=update_bbd, cron_schedule="20 07 * * *", execution_timezone='Asia/Bangkok'
)


bbd2 = ScheduleDefinition(
    name="bbd2", job=update_bbd, cron_schedule="00 17 * * *", execution_timezone='Asia/Bangkok'
)

bbd3 = ScheduleDefinition(
    name="bbd3", job=update_bbd, cron_schedule="57 23 * * *", execution_timezone='Asia/Bangkok'
)

defs = Definitions(
    assets=[get_time, oee_BBD_Insert, dt_BBD_Insert, dtlog_BBD_Insert, qc_BBD_Insert, oee_trend_BBD_Insert],
    schedules=[bbd1, bbd2, bbd3] 
)
