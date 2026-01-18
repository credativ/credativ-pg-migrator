CREATE OR REPLACE FUNCTION dbo.acc2035c_getPermissionStatus(locvar_propertyTypeId NUMERIC(10,0), locvar_propertyValue VARCHAR(255), locvar_permissionStatus NUMERIC(10,0) out)
RETURNS void AS $$
DECLARE
locvar_OptIn NUMERIC(10,0);
locvar_OptOut NUMERIC(10,0);
locvar_AusdruecklicheEinwilligungTelefonisch VARCHAR(10);
locvar_AusdruecklicheEinwilligungOnline VARCHAR(10);
locvar_MutmasslicheEinwilligung VARCHAR(10);
locvar_Firmenkunde VARCHAR(10);
locvar_Unbekannt VARCHAR(10);
locvar_KeineEinwilligung VARCHAR(10);
locvar_AusdruecklicheAblehnung VARCHAR(10);
BEGIN
locvar_OptIn = 1;
locvar_OptOut := 2;
locvar_AusdruecklicheEinwilligungTelefonisch := 10;
locvar_AusdruecklicheEinwilligungOnline := 11;
locvar_MutmasslicheEinwilligung := 20;
locvar_Firmenkunde := 30;
locvar_Unbekannt := 40;
locvar_KeineEinwilligung := 50;
locvar_AusdruecklicheAblehnung := 60;
locvar_permissionStatus := null;
CASE WHEN (locvar_propertyTypeId IN (77, 79, 82, 85, 100, 102, 110, 111)) /* OptOut only */ THEN locvar_permissionStatus := locvar_OptOut ELSE CASE WHEN (locvar_propertyTypeId IN (117, 118, 119, 120)) /* OptIn or OptOut */ THEN CASE WHEN (locvar_propertyValue IN (locvar_AusdruecklicheEinwilligungTelefonisch, locvar_AusdruecklicheEinwilligungOnline)) /* OptIn */ THEN locvar_permissionStatus := locvar_OptIn ELSE CASE WHEN (locvar_propertyValue = locvar_AusdruecklicheAblehnung) /* OptOut */ THEN locvar_permissionStatus := locvar_OptOut END END; ELSE CASE WHEN (locvar_propertyTypeId = 115 AND SUBSTRING(locvar_propertyValue FROM 8 FOR 1) = '1') /* Vertriebskontaktstatus Freenet */ THEN locvar_permissionStatus := locvar_OptOut ELSE CASE WHEN (locvar_propertyTypeId IN (132, 133, 134)) THEN locvar_permissionStatus := null END END END END;
END;
$$ LANGUAGE plpgsql;