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
			converted_code = informix_connector.convert_trigger(tested_code, settings )

	print("***********************************************************************************")
	print(f"{source_db_type} {code_type} Code:")
	print(tested_code)
	print("***********************************************************************************")
	print("Converted PostgreSQL Code:")
	print(converted_code)

if __name__ == "__main__":
	source_db_type = 'informix'
	source_schema = 'sysa'
	target_schema = 'sinter'
	code_type = 'trigger'
	table_list = []
	tested_code = """
create trigger "sysa".upd_smu update of ismu010,ismu020,ismu040,ismu050,ismu130,stoffnummer,eigenschaft on "sysa".smu referencing old as pre new as post                                                                                                            for each row
        when (((pre.ismu010 != post.ismu010 ) OR (pre.ismu020 != post.ismu020 ) ) )
            (
            --
            -- Update der schluesselfelder "Zeitpunkt" (ismu010) und "Bunkernummer"
            -- (ismu020) ist nicht zulaessig
            --
            execute procedure hkmmaster:"sysa".raise_exception('06300' ,'Trigger upd_smu: Schluesselupdate verboten' )),
        when (((((pre.ismu010 = post.ismu010 ) AND (pre.ismu020 = post.ismu020 ) ) AND (pre.stoffnummer IN (996 ,997 ,998 )) ) AND (((((pre.ismu040 != post.ismu040 ) OR (pre.stoffnummer != post.stoffnummer ) ) OR ((pre.ismu060 IS NULL ) AND (post.ismu060 IS NOT NULL ) ) ) OR ((pre.ismu060 IS NOT NULL ) AND (post.ismu060 IS NULL ) ) ) OR (pre.ismu060 != post.ismu060 ) ) ) )
            (
            --
            -- Es werden nur die Bunkerabrechnungssaetze fuer die Mischgut-
            -- saetze beruecksichtigt.
            -- Bei den Daten, vom Leitsystem kommen, sind dies die Stoffschluessel
            -- 996 (Mischbett A), 997 (Mischbett B), 998 (Mischbett C)
            --
            --
            -- Update wg. Mischbett- oder Stoffwechsel oder Wechsel der Herkunft (zugefahrenes Mischgut):
            -- vorhandene Menge fuer alten Schluessel aus der Abhaldung oder Zufahrt loeschen
            -- neue Menge fuer neue Schluessel einfuegen
            --
            execute procedure vorfeld:"sysa".chg_abhaldung(pre.ismu010 ,pre.ismu040 ,hkmmaster:"sysa".mengendiff(pre.ismu050 ,0 ),pre.ismu060 )),
        when (((((pre.ismu010 = post.ismu010 ) AND (pre.ismu020 = post.ismu020 ) ) AND (post.stoffnummer IN (996 ,997 ,998 )) ) AND (((((pre.ismu040 != post.ismu040 ) OR (pre.stoffnummer != post.stoffnummer ) ) OR ((pre.ismu060 IS NULL ) AND (post.ismu060 IS NOT NULL ) ) ) OR ((pre.ismu060 IS NOT NULL ) AND (post.ismu060 IS NULL ) ) ) OR (pre.ismu060 != post.ismu060 ) ) ) )
            (
            execute procedure vorfeld:"sysa".chg_abhaldung(post.ismu010 ,post.ismu040 ,hkmmaster:"sysa".mengendiff(0 ,post.ismu050 ),post.ismu060 )),
        when (((((((pre.ismu010 = post.ismu010 ) AND (pre.ismu020 = post.ismu020 ) ) AND (pre.ismu040 = post.ismu040 ) ) AND (pre.stoffnummer = post.stoffnummer ) ) AND (post.stoffnummer IN (996 ,997 ,998 )) ) AND (pre.ismu050 != post.ismu050 ) ) )
            (
            --
            -- Update wg. manueller Aenderung der Menge
            --
            execute procedure vorfeld:"sysa".chg_abhaldung(post.ismu010 ,post.ismu040 ,hkmmaster:"sysa".mengendiff(pre.ismu050 ,post.ismu050 ),post.ismu060 )),
        when (((((((pre.ismu010 = post.ismu010 ) AND (pre.ismu020 = post.ismu020 ) ) AND (pre.ismu040 = post.ismu040 ) ) AND (pre.stoffnummer = post.stoffnummer ) ) AND (post.stoffnummer IN (996 ,997 ,998 )) ) AND (post.ismu130 = 'L' ) ) )
            (
            --
            -- Update wg. manuellem Loeschen eines Satzes (Loeschkennzeichen 'L')
            --
            execute procedure vorfeld:"sysa".chg_abhaldung(post.ismu010 ,post.ismu040 ,hkmmaster:"sysa".mengendiff(post.ismu050 ,0 ),post.ismu060 ),
            execute procedure vorfeld:"sysa".chk_mb_kenndaten(vorfeld:"sysa".get_mb_id(post.ismu040 ,post.ismu010 ,'ABH' ),'ABH' )),
        when ((pre.ismu020 IN ('03' ,'04' )) )
            (
            --
            -- Auftrag fuer Bilanzierung Brennstoffe in den Bunkern
            --
            -- Ftj_telsend sorgt dafuer, dass keine doppelten Auftraege geschrieben
            -- werden
            --
            execute procedure global:"sysa".must_write_600_bb(pre.ismu010 ,'smu' )),
        when ((post.ismu020 IN ('03' ,'04' )) )
            (
            execute procedure global:"sysa".must_write_600_bb(post.ismu010 ,'smu' )),
        when (((((((post.ismu010 IS NOT NULL ) AND (post.ismu040 IS NOT NULL ) ) AND (post.ismu020 IN ('05' ,'09' ,'10' ,'11' ,'19' ,'101' ,'111' )) ) AND (post.stoffnummer NOT IN (996 ,997 ,998 )) ) AND (post.eigenschaft IS NULL ) ) AND ((post.ismu130 != 'L' ) OR (post.ismu130 IS NULL ) ) ) )
            (
            --
            -- Bunkerabzuege der Zudosierbunker 5,9,10,11,19,101 und 111 betrachten:
            --
            -- ueber einen View zu Mischbett-/Monatswerten verdichten
            --
            insert into "sysa".changes (sid,tabelle,zeit,info)  values (DBINFO ('sessionid') ,'smu' ,pre.ismu010 ,pre.ismu040 ),
            insert into "sysa".changes (sid,tabelle,zeit,info)  values (DBINFO ('sessionid') ,'smu' ,post.ismu010 ,post.ismu040 ))
    after
        (
        delete from "sysa".smb_n  where (((monat >= (select (min(x0.zeit ) - 5356800. ) from "sysa".changes x0 where (x0.sid = DBINFO ('sessionid') ) ) ) AND (monat <= (select (max(x1.zeit ) + 5356800. ) from "sysa".changes x1 where (x1.sid = DBINFO ('sessionid') ) ) ) ) AND (mischbett = ANY (select x2.info from "sysa".changes x2 where (x2.sid = DBINFO ('sessionid') ) ) ) ) ,
        insert into "sysa".smb_n (monat,mischbett,bunker,stoff,menge) select x3.monat ,x3.mischbett ,x3.bunker ,x3.stoff ,x3.menge from "sysa".v_zudosierung x3 where (x3.mischbett = ANY (select x4.info from "sysa".changes x4 where (x4.sid = DBINFO ('sessionid') ) ) ) ,
        delete from "sysa".changes  where (sid = DBINFO ('sessionid') ) );
"""
	main(tested_code, code_type, source_db_type, source_schema, target_schema, table_list)
