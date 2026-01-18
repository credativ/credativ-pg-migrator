CREATE PROCEDURE dbo.acc2035c_getPermissionStatus
(
	@propertyTypeId numeric(10,0),
	@propertyValue univarchar(255),
	@permissionStatus numeric(10,0) out
)
as
begin

	declare
		@OptIn numeric(10,0),
		@OptOut numeric(10,0),
		@AusdruecklicheEinwilligungTelefonisch univarchar(10),
		@AusdruecklicheEinwilligungOnline univarchar(10),
		@MutmasslicheEinwilligung univarchar(10),
		@Firmenkunde univarchar(10),
		@Unbekannt univarchar(10),
		@KeineEinwilligung univarchar(10),
		@AusdruecklicheAblehnung univarchar(10)


	select
		@OptIn = 1,
		@OptOut = 2,
		@AusdruecklicheEinwilligungTelefonisch = '10',
		@AusdruecklicheEinwilligungOnline = '11',
		@MutmasslicheEinwilligung = '20',
		@Firmenkunde = '30',
		@Unbekannt = '40',
		@KeineEinwilligung = '50',
		@AusdruecklicheAblehnung = '60'

    select
    	@permissionStatus = null

    if(@propertyTypeId in (77,79,82,85,100,102,110,111)) -- OptOut only
        select @permissionStatus = @OptOut

    else if(@propertyTypeId in (117,118,119,120)) -- OptIn or OptOut
        begin
            if(@propertyValue in (@AusdruecklicheEinwilligungTelefonisch, @AusdruecklicheEinwilligungOnline)) --OptIn
                select @permissionStatus = @OptIn
            else if (@propertyValue = @AusdruecklicheAblehnung) -- OptOut
                select @permissionStatus = @OptOut
        end
    else if(@propertyTypeId = 115 and SUBSTRING(@propertyValue,8,1) = '1') -- Vertriebskontaktstatus Freenet
        select @permissionStatus = @OptOut

    else if(@propertyTypeId in (132, 133, 134))
        select @permissionStatus = null

end
