CREATE OR REPLACE FUNCTION dbo.acc2035c_getPermissionStatus(
    IN locvar_propertyTypeId NUMERIC(10,0),
    IN locvar_propertyValue VARCHAR(255),
    INOUT locvar_permissionStatus NUMERIC(10,0)
)
RETURNS NUMERIC(10,0)
LANGUAGE plpgsql
AS $$
DECLARE
    locvar_OptIn NUMERIC(10,0);
BEGIN

		locvar_OptOut numeric(10,0),
		locvar_AusdruecklicheEinwilligungTelefonisch univarchar(10),
		locvar_AusdruecklicheEinwilligungOnline univarchar(10),
		locvar_MutmasslicheEinwilligung univarchar(10),
		locvar_Firmenkunde univarchar(10),
		locvar_Unbekannt univarchar(10),
		locvar_KeineEinwilligung univarchar(10),
		locvar_AusdruecklicheAblehnung univarchar(10)


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

    if((locvar_propertyTypeId in (77,79,82,85,100,102,110,111)) THEN) /*OptOut only*/
        locvar_permissionStatus := locvar_OptOut;

    ELSIF((locvar_propertyTypeId in (117,118,119,120)) THEN) /*OptIn or OptOut*/
        begin
            if((locvar_propertyValue in (locvar_AusdruecklicheEinwilligungTelefonisch, locvar_AusdruecklicheEinwilligungOnline)) THEN) /*OptIn*/
                locvar_permissionStatus := locvar_OptIn;
            ELSIF((locvar_propertyValue = locvar_AusdruecklicheAblehnung)) THEN /*OptOut*/
                locvar_permissionStatus := locvar_OptOut;
        END;
    ELSIF((locvar_propertyTypeId = 115 and SUBSTRING(locvar_propertyValue,8,1)) THEN = '1') /*Vertriebskontaktstatus Freenet*/
        locvar_permissionStatus := locvar_OptOut;

    ELSIF((locvar_propertyTypeId in (132, 133, 134)) THEN)
        locvar_permissionStatus := null;
END;
$$;
