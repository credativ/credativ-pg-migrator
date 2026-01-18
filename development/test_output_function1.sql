CREATE OR REPLACE FUNCTION dbo.acc2035c_getPermissionStatus(
    IN locvar_locvar_propertyTypeId NUMERIC(10,0),
    IN locvar_locvar_propertyValue VARCHAR(255),
    INOUT locvar_locvar_permissionStatus_1 NUMERIC(10,0)
)
RETURNS TABLE
LANGUAGE plpgsql
AS $$
DECLARE
    locvar_OptIn_1 NUMERIC(10,0);
BEGIN

		locvar_OptOut_1 numeric(10,0),
		locvar_AusdruecklicheEinwilligungTelefonisch_1 univarchar(10),
		locvar_AusdruecklicheEinwilligungOnline_1 univarchar(10),
		locvar_MutmasslicheEinwilligung_1 univarchar(10),
		locvar_Firmenkunde_1 univarchar(10),
		locvar_Unbekannt_1 univarchar(10),
		locvar_KeineEinwilligung_1 univarchar(10),
		locvar_AusdruecklicheAblehnung_1 univarchar(10)


	locvar_OptIn := 1;
locvar_OptOut := 2;
locvar_AusdruecklicheEinwilligungTelefonisch := '10';
locvar_AusdruecklicheEinwilligungOnline := '11';
locvar_MutmasslicheEinwilligung := '20';
locvar_Firmenkunde := '30';
locvar_Unbekannt := '40';
locvar_KeineEinwilligung := '50';
locvar_AusdruecklicheAblehnung := '60';
    locvar_permissionStatus := null;
    if((locvar_propertyTypeId in (77,79,82,85,100,102,110,111)) THEN) /* OptOut only*/
        locvar_permissionStatus := locvar_OptOut_1;
    else if((locvar_propertyTypeId in (117,118,119,120)) THEN) /* OptIn or OptOut*/
        begin
            if((locvar_propertyValue in (locvar_AusdruecklicheEinwilligungTelefonisch_1, locvar_AusdruecklicheEinwilligungOnline_1)) THEN) /*OptIn*/
                locvar_permissionStatus := locvar_OptIn_1;
            else if((locvar_propertyValue = locvar_AusdruecklicheAblehnung_1)) THEN /* OptOut*/
                locvar_permissionStatus := locvar_OptOut_1;
        END;
    else if((locvar_propertyTypeId = 115 and SUBSTRING(locvar_propertyValue,8,1)) THEN = '1') /* Vertriebskontaktstatus Freenet*/
        locvar_permissionStatus := locvar_OptOut_1;
    else if((locvar_propertyTypeId in (132, 133, 134)) THEN)
        locvar_permissionStatus := null;
END;
$$;
