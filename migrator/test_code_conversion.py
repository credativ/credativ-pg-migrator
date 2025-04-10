from informix_connector import InformixConnector
from sybase_ase_connector import SybaseASEConnector
from postgresql_connector import PostgreSQLConnector
from configparser import ConfigParser

class MockConfigParser:
    def get_on_error_action(self):
        return 'stop'

    def get_log_file(self):
        return 'test.log'

    def get_log_level(self):
        return 'DEBUG'

    def get_indent(self):
        return '    '

def main(tested_code, code_type, source_db_type, source_schema, target_schema, table_list):

	# Initialize the InformixConnector with a mock config parser
	config_parser = MockConfigParser()

	if source_db_type == 'informix':
		informix_connector = InformixConnector(config_parser, 'source')

		if code_type == 'funcproc':
			# Convert the Informix procedure code to PostgreSQL
			converted_code = informix_connector.convert_funcproc_code(
				funcproc_code=tested_code,
				target_db_type='postgresql',
				source_schema=source_schema,
				target_schema=target_schema,
				table_list=['pack', "pack1", 'pack2', 'pack3', 'prodname']
			)
		elif code_type == 'trigger':
			print("Converting trigger code...")
			settings = {
				'source_schema': source_schema,
				'target_schema': target_schema
			}
			converted_code = informix_connector.convert_trigger(
				trig=tested_code,
				settings=settings
			)

	# print("***********************************************************************************")
	# print(f"{source_db_type} {code_type} Code:")
	# print(tested_code)
	print("***********************************************************************************")
	print("Converted PostgreSQL Code:")
	print(converted_code)

if __name__ == "__main__":
	source_db_type = 'informix'
	source_schema = 'source_schema'
	target_schema = 'target_schema'
	code_type = 'trigger'
	table_list = []
# 	tested_code = """
# create trigger "sysa".del_smu delete on "sysa".smu referencing old as prev                                                                                                                                                                                          for each row
#         when (prev.stoffnummer IN (996 ,997 ,998 ))
#             (
#             --
#             -- Abhaldungmengen des Mischbetts betrachten:
#             --
#             -- Es werden nur die Bunkerabrechnungssaetze fuer die Mischgut-
#             -- saetze beruecksichtigt.
#             -- Bei den Daten, die vom Leitsystem kommen, sind dies die Stoffschluessel
#             -- 996 (Mischbett A), 997 (Mischbett B), 998 (Mischbett C)
#             --
#             execute procedure vorfeld:"sysa".chg_abhaldung(prev.ismu010 ,prev.ismu040 ,hkmmaster:"sysa".mengendiff(prev.ismu050 ,0 ),prev.ismu060 ,1 )),
#         when ((prev.ismu020 IN ('03' ,'04' )) )
#             (
#             --
#             -- Bunkerabzuege Bunker 3 und 4 betrachten:
#             --
#             -- Auftrag zur Bilanzierung der Brennstoffe in den Bunkern
#             --
#             execute procedure global:"sysa".must_write_600_bb(prev.ismu010 ,'smu' )),
#         when ((((((((prev.ismu010 IS NOT NULL ) AND (prev.ismu040 IS NOT NULL ) ) AND ((DBINFO ('utc_current') - prev.ismu010 ) < 17280000. ) ) AND (prev.ismu020 IN ('05' ,'09' ,'10' ,'11' ,'19' ,'101' ,'111' )) ) AND (prev.stoffnummer NOT IN (996 ,997 ,998 )) ) AND (prev.eigenschaft IS NULL ) ) AND ((prev.ismu130 != 'L' ) OR (prev.ismu130 IS NULL ) ) ) )
#             (
#             --
#             -- Bunkerabzuege der Zudosierbunker 5,9,10,11,19,101 und 111 betrachten:
#             --
#             -- ueber einen View zu Mischbett-/Monatswerten verdichten
#             --
#             insert into "sysa".changes (sid,tabelle,zeit,info)  values (DBINFO ('sessionid') ,'smu' ,prev.ismu010 ,prev.ismu040 ))
#     after
#         (
#         delete from "sysa".smb_n  where (((monat >= (select (min(x0.zeit ) - 5356800. ) from "sysa".changes x0 where (x0.sid = DBINFO ('sessionid') ) ) ) AND (monat <= (select (max(x1.zeit ) + 5356800. ) from "sysa".changes x1 where (x1.sid = DBINFO ('sessionid') ) ) ) ) AND (mischbett = ANY (select x2.info from "sysa".changes x2 where (x2.sid = DBINFO ('sessionid') ) ) ) ) ,
#         insert into "sysa".smb_n (monat,mischbett,bunker,stoff,menge) select x3.monat ,x3.mischbett ,x3.bunker ,x3.stoff ,x3.menge from "sysa".v_zudosierung x3 where (x3.mischbett = ANY (select x4.info from "sysa".changes x4 where (x4.sid = DBINFO ('sessionid') ) ) ) ,
#         delete from "sysa".changes  where (sid = DBINFO ('sessionid') ) );
#     """
# 	tested_code = """
# create trigger "sysa".del_fms delete on "sysa".fms referencing old as prev                                                                                                                                                                                          for each row
#         when ((((prev.ifms110 LIKE 'A%%' ) OR (prev.ifms110 LIKE 'B%%' ) ) OR (prev.ifms110 LIKE 'C%%' ) ) )
#             (
#             execute procedure "sysa".del_fms(prev.ifms010 ,prev.ifms110 ));
# """
	tested_code = """
create trigger "sysa".ins_smu insert on "sysa".smu referencing new as post                                                                                                                                                                                          for each row
        when ((post.stoffnummer IN (996 ,997 ,998 )) )
            (
            --
            -- Abhaldungmengen des Mischbetts betrachten:
            --
            -- Es werden nur die Bunkerabrechnungssaetze fuer die Mischgut-
            -- saetze beruecksichtigt.
            -- Bei den Daten, vom Leitsystem kommen, sind dies die Stoffschluessel
            -- 996 (Mischbett A), 997 (Mischbett B), 998 (Mischbett C)
            --
            execute procedure vorfeld:"sysa".chg_abhaldung(post.ismu010 ,post.ismu040 ,hkmmaster:"sysa".mengendiff(0 ,post.ismu050 ),post.ismu060 )),
        when ((post.ismu020 IN ('03' ,'04' )) )
            (
            --
            -- Bunkerabzuege Bunker 3 und 4 betrachten:
            --
            -- Auftrag zur Bilanzierung der Brennstoffe in den Bunkern
            --
            execute procedure global:"sysa".must_write_600_bb(post.ismu010 ,'smu' )),
        when (((((((post.ismu010 IS NOT NULL ) AND (post.ismu040 IS NOT NULL ) ) AND (post.ismu020 IN ('05' ,'09' ,'10' ,'11' ,'19' ,'101' ,'111' )) ) AND (post.stoffnummer NOT IN (996 ,997 ,998 )) ) AND (post.eigenschaft IS NULL ) ) AND ((post.ismu130 != 'L' ) OR (post.ismu130 IS NULL ) ) ) )
            (
            --
            -- Bunkerabzuege der Zudosierbunker 5,9,10,11,19,101 und 111 betrachten:
            --
            -- ueber einen View zu Mischbett-/Monatswerten verdichten
            --
            insert into "sysa".changes (sid,tabelle,zeit,info)  values (DBINFO ('sessionid') ,'smu' ,post.ismu010 ,post.ismu040 ))
    after
        (
        delete from "sysa".smb_n  where (((monat >= (select (min(x0.zeit ) - 5356800. ) from "sysa".changes x0 where (x0.sid = DBINFO ('sessionid') ) ) ) AND (monat <= (select (max(x1.zeit ) + 5356800. ) from "sysa".changes x1 where (x1.sid = DBINFO ('sessionid') ) ) ) ) AND (mischbett = ANY (select x2.info from "sysa".changes x2 where (x2.sid = DBINFO ('sessionid') ) ) ) ) ,
        insert into "sysa".smb_n (monat,mischbett,bunker,stoff,menge) select x3.monat ,x3.mischbett ,x3.bunker ,x3.stoff ,x3.menge from "sysa".v_zudosierung x3 where (x3.mischbett = ANY (select x4.info from "sysa".changes x4 where (x4.sid = DBINFO ('sessionid') ) ) ) ,
        delete from "sysa".changes  where (sid = DBINFO ('sessionid') ) );
	"""
	# Call the main function with the test code and parameters
	main(tested_code, code_type, source_db_type, source_schema, target_schema, table_list)