from asyncore import read
import pyodbc

class DBHelperBBD:

    def __init__(self):
        self.server = '10.35.101.148,49170'
        database = 'BBD'
        user = 'sa'
        password = 'user001'
        conn_info = 'DRIVER={ODBC Driver 17 for SQL Server};DATABASE=%s;SERVER=%s;UID=%s;PWD=%s' % (database, self.server, user, password)
        self.connection = pyodbc.connect(conn_info, unicode_results=True)
        self.cursor = self.connection.cursor()

    # GET DATA OEE BBD
    def get_oeeBBD(self,datecomplete,shift):
        self.cursor.execute("SELECT * FROM [BBD].[dbo].[oee_BBD] where id like '"+str(datecomplete)+"%"+str(shift)+"' ")
        return self.cursor.fetchall()
    
    # GET DATA DT BBD
    def get_dt_BBD(self,datecomplete,shift):
        self.cursor.execute("SELECT * FROM [BBD].[dbo].[dt_BBD] where id like '"+str(datecomplete)+"%"+str(shift)+"' ")
        return self.cursor.fetchall()

    # GET DATA DT LOG BBD
    def get_dtlog_BBD(self,datecomplete,shift):
        self.cursor.execute("SELECT * FROM [BBD].[dbo].[dtlog_BBD] where id like '"+str(datecomplete)+"%"+str(shift)+"' ")
        return self.cursor.fetchall()

     # GET OEE TREND BBD
    def get_oeeTrendBBD(self,datecomplete):
        self.cursor.execute("SELECT * FROM [BBD].[dbo].[oee_trend_BBD] WHERE CONVERT(date, Datetime) LIKE '"+str(datecomplete)+"%' ")
        return self.cursor.fetchall()

    # GET DATA QC BBD
    def get_qcBBD(self,datecomplete,shift):
        self.cursor.execute("SELECT * FROM [BBD].[dbo].[qc_BBD] where id like '"+str(datecomplete)+"%"+str(shift)+"' ")
        return self.cursor.fetchall()

class DBHelperBBD2:
    def __init__(self):
        self.server = 'APCKRMPTMD01TV,41433'
        database = 'BBD'
        user = 'ptmiiot'
        password = 'ptmiiot@123'
        conn_info = 'DRIVER={ODBC Driver 17 for SQL Server};DATABASE=%s;SERVER=%s;UID=%s;PWD=%s' % (database, self.server, user, password)
        self.connection = pyodbc.connect(conn_info, unicode_results=True)
        self.cursor = self.connection.cursor()

    # def check_oeeBBD(self,datecomplete,shift):
    #     self.cursor.execute("SELECT TOP(1) * FROM [BBD].[dbo].[oee_BBD] where id like '"+str(datecomplete)+"%"+str(shift)+"' ")
    #     return self.cursor.fetchall()

    # def check_dt_BBD(self,datecomplete,shift):
    #     self.cursor.execute("SELECT TOP(1) * FROM [BBD].[dbo].[dt_BBD] where id like '"+str(datecomplete)+"%"+str(shift)+"' ")
    #     return self.cursor.fetchall()

    # def check_dtlog_BBD(self,datecomplete,shift):
    #     self.cursor.execute("SELECT TOP(1) * FROM [BBD].[dbo].[dtlog_BBD] where id like '"+str(datecomplete)+"%"+str(shift)+"' ")
    #     return self.cursor.fetchall()

    # def check_qcBBD(self,datecomplete,shift):
    #     self.cursor.execute("SELECT TOP(1) * FROM [BBD].[dbo].[qc_BBD] where id like '"+str(datecomplete)+"%"+str(shift)+"' ")
    #     return self.cursor.fetchall()

    # def check_realtime_BBD_hourly(self,datecomplete, shift):
    #     self.cursor.execute("SELECT TOP(1) * FROM [BBD].[dbo].[realtime_BBD_hourly] WHERE id like '%"+str(datecomplete)+"%' AND shift like '%"+str(shift)+"%' ")
    #     return self.cursor.fetchall()

    # INSERT OEE BBD
    def insert_oee_BBD(self, oeeBBD_id, oeeBBD_runtime, oeeBBD_downtime, oeeBBD_uptime,oeeBBD_stdCT, oeeBBD_actCT, oeeBBD_planoutput, oeeBBD_good, oeeBBD_reject,  oeeBBD_output,  oeeBBD_availability, oeeBBD_performance, oeeBBD_quality, oeeBBD_oee, oeeBBD_utilization):
        self.cursor.execute(
            """
            INSERT INTO [BBD].[dbo].[oee_BBD]
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, oeeBBD_id, oeeBBD_runtime, oeeBBD_downtime, oeeBBD_uptime,oeeBBD_stdCT, oeeBBD_actCT, oeeBBD_planoutput,  oeeBBD_good,  oeeBBD_reject, oeeBBD_reject,  oeeBBD_availability, oeeBBD_performance, oeeBBD_quality, oeeBBD_oee, oeeBBD_utilization
        )
        return self.cursor.commit()

    # INSERT DT BBD
    def insert_dt_BBD(self, dtBBD_id, dtBBD_toolproblem, dtBBD_paintproblem, dtBBD_nomaterial, dtBBD_finishschedule, dtBBD_noproduction, dtBBD_emergency, dtBBD_preventivemaintenance, dtBBD_changeover, dtBBD_refreshpaint, dtBBD_changeovermask, dtBBD_total):
        self.cursor.execute(
            """
            INSERT INTO [BBD].[dbo].[dt_BBD]
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, dtBBD_id, dtBBD_toolproblem, dtBBD_paintproblem, dtBBD_nomaterial, dtBBD_finishschedule, dtBBD_noproduction, dtBBD_emergency, dtBBD_preventivemaintenance, dtBBD_changeover, dtBBD_refreshpaint, dtBBD_changeovermask, dtBBD_total
        )
        return self.cursor.commit()

    # INSERT DT LOG BBD
    def insert_dtlog_BBD(self, dtlogBBD_id, dtlogBBD_dtstatus):
        self.cursor.execute(
            """
            INSERT INTO [BBD].[dbo].[dtlog_BBD]
            VALUES (?,?)
            """, dtlogBBD_id, dtlogBBD_dtstatus
        )
        return self.cursor.commit()

    # INSERT TREND DATA BBD
    def insert_oee_trend_BBD(self, oee_trend_1, oee_trend_2):
        self.cursor.execute(
            """
            INSERT INTO [BBD].[dbo].[oee_trend_BBD]
            VALUES (?,?)
            """, oee_trend_1, oee_trend_2
        )
        return self.cursor.commit()

    # INSERT QC BBD
    def insert_qc_BBD(self, qcBBD_id, qcBBD_good, qcBBD_reject,  qcBBD_total, qcBBD_asymetricaleye, qcBBD_mispositionpaintltcc, qcBBD_scratchpaint, qcBBD_peeloff, qcBBD_contaminationpaint, qcBBD_wrongcolor, qcBBD_missingpaint, qcBBD_dirty, qcBBD_maskmark, qcBBD_blackspot, qcBBD_bubble, qcBBD_deform, qcBBD_flesh): 
        self.cursor.execute(
            """
            INSERT INTO [BBD].[dbo].[qc_BBD]
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, qcBBD_id, qcBBD_good, qcBBD_reject,  qcBBD_total, qcBBD_asymetricaleye, qcBBD_mispositionpaintltcc, qcBBD_scratchpaint, qcBBD_peeloff, qcBBD_contaminationpaint, qcBBD_wrongcolor, qcBBD_missingpaint, qcBBD_dirty, qcBBD_maskmark, qcBBD_blackspot, qcBBD_bubble, qcBBD_deform, qcBBD_flesh
        )
        return self.cursor.commit()

    
