from sqlalchemy.engine import create_engine

# build a connection with database

engine = create_engine('mysql://vivauser:q5dD>7@213.202.255.197:3306/vivaneo_staging')

conn = engine.connect()

# create the mapping table

query = '''drop table if exists mapping_therapy_treatment;'''

conn.execute(query)

query = '''create table mapping_therapy_treatment(
           treatment_group text, 
           DURCHG_BEH_ID text);'''

conn.execute(query)

query = '''insert into mapping_therapy_treatment values 
             ("Cryo", "Kryo-Versuch"),
             ("Pkt.-Transfer", "Pkt.-Transfer"), 
             ("IUI", "Hom.IUI"), 
             ("IUI", "Het.IUI"),
             ("Eizell-Spende", "Eizell-Spende"),
             
             ("Cryo","Auftau"),
             ("IUI", "Insem (homolog)"), 
             ("IUI", "Insem (heterolog)"),
             ("Punktion", "IVF"),
             ("Punktion", "ICSI"),
             ("Punktion", "IVF,ICSI"), 
             ("Punktion", "Nur Punktion"),
             ("VZO", "VZO"), 
             ("nicht durchgef\xfchrt", "nicht durchgef\xfchrt"),
             ("Schwangerschaftsbeobachtung", "Schwangerschaftsbeobachtung")'''

conn.execute(query)

# create a tempory merge table for different data source

query = '''drop table if exists therapy_temp1;'''

conn.execute(query)

query = '''create table therapy_temp1(
           GEPL_BEH_ID text, 
           DURCHG_BEH_ID text,
           DURCHG_BEH_ID_DETAILED text,
           BEZUGSDATUM date,
           BEZUGSDATUM_TYP text,
           BEHANDLER_ID text,
           STANDORT_ID text,
           location_id text,
           DATE_ET_IUI date, DATE_PUNKTION date, DATE_AUFTAU date, 
           THERAPIE_ID int,
           FRAU_ID int, 
           MANN_ID int,
           THERAPIE_NR int, 
           PATIENT_ID varchar(20), 
           PATIENT_ID_PARTNER varchar(20), 
           ALTER_AM_STICHTAG text, 
           ALTER_AM_STICHTAG_PARTNER text,
           DT_LP date,
           SS text, SS_WOCHE text, treatment_group text);'''

conn.execute(query)

# get data from austria
# current man can fill only some particual columns

query = '''insert into therapy_temp1 (DURCHG_BEH_ID, BEZUGSDATUM, location_id, DATE_ET_IUI, DATE_PUNKTION, DATE_AUFTAU, 
           THERAPIE_ID, FRAU_ID, MANN_ID, THERAPIE_NR, PATIENT_ID, PATIENT_ID_PARTNER, ALTER_AM_STICHTAG, DT_LP, 
           SS, SS_WOCHE, treatment_group)
           select distinct Behandlungsart,
           if(BehandlungsBeginn is not null, str_to_date(BehandlungsBeginn, "%%d.%%m.%%Y"), 
           str_to_date("1.1.1900", "%%d.%%m.%%Y")), 
           "WLS",  
            if(TransferDatum is not null, str_to_date(TransferDatum, "%%d.%%m.%%Y"), 
             str_to_date("1.1.1900", "%%d.%%m.%%Y")), 
             if(ivf <> 0 or icsi <> 0,  
             if(BehandlungsDatum is not null, str_to_date(BehandlungsDatum, "%%d.%%m.%%Y"), 
             str_to_date("1.1.1900", "%%d.%%m.%%Y")),  str_to_date("1.1.1900", "%%d.%%m.%%Y")),
             if(AuftauDatum is not null, str_to_date(AuftauDatum, "%%d.%%m.%%Y"), 
             str_to_date("1.1.1900", "%%d.%%m.%%Y")), 
             LaborID, PatientenID, partnerid,
             Zyklus, PatientenID, partnerid, PatientinAlter,
             if(BehandlungsDatum is not null, str_to_date(BehandlungsDatum, "%%d.%%m.%%Y"), 
             str_to_date("1.1.1900", "%%d.%%m.%%Y")), 
             Schwangerschaft, 
             if(SchwangerschaftsTestAm is not null, str_to_date(SchwangerschaftsTestAm, "%%d.%%m.%%Y"), 
             str_to_date("1.1.1900", "%%d.%%m.%%Y")),
             t2.treatment_group from therapie_at t1, mapping_therapy_treatment t2
             where t1.Behandlungsart = t2.durchg_beh_id;'''

conn.execute(query)

query = '''drop table if exists therapy;'''

conn.execute(query)

query = '''create table therapy(
           GEPL_BEH_ID text, 
           DURCHG_BEH_ID text,
           DURCHG_BEH_ID_DETAILED text,
           BEZUGSDATUM date,
           BEZUGSDATUM_TYP text,
           BEHANDLER_ID text,
           STANDORT_ID text,
           location_id text,
           DATE_ET_IUI date, DATE_PUNKTION date, DATE_AUFTAU date, 
           THERAPIE_ID int,
           FRAU_ID int, 
           MANN_ID int,
           THERAPIE_NR int, 
           PATIENT_ID varchar(20), 
           PATIENT_ID_PARTNER varchar(20), 
           ALTER_AM_STICHTAG text, 
           ALTER_AM_STICHTAG_PARTNER text,
           DT_LP date,
           SS text, SS_WOCHE text, treatment_group text,
           row_number int, punktion_nr int);'''

conn.execute(query)

# Zaehler
# with sorted data, group_row_number = group_max_row_number - row_number + group_volumn
# man must coordinate column name every time with individual response.

query = '''insert into therapy (DURCHG_BEH_ID, BEZUGSDATUM, LOCATION_ID, DATE_ET_IUI, DATE_PUNKTION, DATE_AUFTAU, THERAPIE_ID,
           FRAU_ID, MANN_ID, THERAPIE_NR, PATIENT_ID, PATIENT_ID_PARTNER, ALTER_AM_STICHTAG, DT_LP, SS, SS_WOCHE,
           treatment_group, row_number, punktion_nr)
           select t1.DURCHG_BEH_ID, t1.BEZUGSDATUM, t1.location_id, t1.DATE_ET_IUI, t1.DATE_PUNKTION, t1.DATE_AUFTAU, t1.THERAPIE_ID,
           t1.FRAU_ID, t1.MANN_ID, t1.THERAPIE_NR, t1.PATIENT_ID, t1.PATIENT_ID_PARTNER, t1.ALTER_AM_STICHTAG, 
           t1.DT_LP, t1.SS, t1.SS_WOCHE, t1.treatment_group, t2.n, t2.rank 
           from therapy_temp1 t1 
           left join (
           select t1.col1, t1.col2, t1.col3, t1.n, t1.n-t3.l+t2.m rank
           from (select col1, col2, col3, @row_num1:=@row_num1+1 n
           from (
           select patient_id col1, treatment_group col2, THERAPIE_ID col3
           from therapy_temp1
           order by patient_id, treatment_group, THERAPIE_ID) t, (
           select @row_num1:=0) v) t1, (
           select patient_id col1, treatment_group col2, count(1) m 
           from therapy_temp1
           group by patient_id, treatment_group) t2, (
           select col1, col2, max(n) l from (
           select col1, col2, col3, @row_num2:=@row_num2+1 n 
           from (
           select patient_id col1, treatment_group col2, THERAPIE_ID col3
           from therapy_temp1
           order by patient_id, treatment_group, THERAPIE_ID) t, (
           select @row_num2:=0) v) s group by col1, col2) t3
           where t1.col1 = t2.col1 and t1.col1 = t3.col1 
             and t1.col2 = t2.col2 and t1.col2 = t3.col2) t2
           on t1.patient_id = t2.col1
           and t1.treatment_group = t2.col2
           and t1.THERAPIE_ID = t2.col3;'''

conn.execute(query)

query = '''drop table if exists therapy_temp1;'''

conn.execute(query)

query = '''drop table if exists therapy_temp2;'''

conn.execute(query)

query = '''create table therapy_temp2(
           GEPL_BEH_ID text, 
           DURCHG_BEH_ID text,
           DURCHG_BEH_ID_DETAILED text,
           BEZUGSDATUM date,
           BEZUGSDATUM_TYP text,
           BEHANDLER_ID text,
           STANDORT_ID text,
           location_id text,
           DATE_ET_IUI date, DATE_PUNKTION date, DATE_AUFTAU date, 
           THERAPIE_ID int,
           FRAU_ID int, 
           MANN_ID int,
           THERAPIE_NR int, 
           PATIENT_ID varchar(20), 
           PATIENT_ID_PARTNER varchar(20), 
           ALTER_AM_STICHTAG text, 
           ALTER_AM_STICHTAG_PARTNER text,
           DT_LP date,
           SS text, SS_WOCHE text, treatment_group text);'''

conn.execute(query)

# get data from austria
# current man can fill only some particual columns

query = '''insert into therapy_temp2
           select GEPL_BEH_ID, t1.DURCHG_BEH_ID, DURCHG_BEH_ID_DETAILED, BEZUGSDATUM, BEZUGSDATUM_TYP, BEHANDLER_ID,
           STANDORT_ID, location_id, DATE_ET_IUI, DATE_PUNKTION, DATE_AUFTAU, 
           THERAPIE_ID, FRAU_ID, MANN_ID, THERAPIE_NR,
           PATIENT_ID, PATIENT_ID_PARTNER, ALTER_AM_STICHTAG, ALTER_AM_STICHTAG_PARTNER, DT_LP, SS, SS_WOCHE, 
           t2.treatment_group
           from meditex_therapy t1, mapping_therapy_treatment t2
             where t1.durchg_beh_id = t2.durchg_beh_id;'''

conn.execute(query)

# Zaehler
# with sorted data, group_row_number = group_max_row_number - row_number + group_volumn
# man must coordinate column name every time with individual response.

query = '''insert into therapy 
           select t1.*, t2.n, t2.rank 
           from therapy_temp2 t1 
           left join (
           select t1.col1, t1.col2, t1.col3, t1.n, t1.n-t3.l+t2.m rank
           from (select col1, col2, col3, @row_num1:=@row_num1+1 n
           from (
           select patient_id col1, treatment_group col2, THERAPIE_ID col3
           from therapy_temp2
           order by patient_id, treatment_group, THERAPIE_ID) t, (
           select @row_num1:=0) v) t1, (
           select patient_id col1, treatment_group col2, count(1) m 
           from therapy_temp2
           group by patient_id, treatment_group) t2, (
           select col1, col2, max(n) l from (
           select col1, col2, col3, @row_num2:=@row_num2+1 n 
           from (
           select patient_id col1, treatment_group col2, THERAPIE_ID col3
           from therapy_temp2
           order by patient_id, treatment_group, THERAPIE_ID) t, (
           select @row_num2:=0) v) s group by col1, col2) t3
           where t1.col1 = t2.col1 and t1.col1 = t3.col1 
             and t1.col2 = t2.col2 and t1.col2 = t3.col2) t2
           on t1.patient_id = t2.col1
           and t1.treatment_group = t2.col2
           and t1.THERAPIE_ID = t2.col3;'''

conn.execute(query)

query = '''drop table if exists therapy_temp2;'''

conn.execute(query)

conn.close