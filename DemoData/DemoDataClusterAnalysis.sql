
DECLARE @parts int = 1;
DECLARE @area varchar(100) = 'Little-Rock-Pine-Bluff'
DECLARE @use_time_decay bit = 0

DECLARE @total_clusters int = (select top 1 total_clusters from For_Demo_Analysis 
								where total_parts=@parts AND use_time_decay=@use_time_decay AND area=@area)

-- Print total households per cluster
SELECT cluster_number,COUNT(0) as households 
FROM For_Demo_Analysis 
WHERE total_parts=@parts AND use_time_decay=@use_time_decay AND area=@area
GROUP BY cluster_number
ORDER BY cluster_number

DECLARE @DemodataSlice TABLE (
	[Household_ID] [varchar](50) NULL,
	[Household_size] [varchar](50) NULL,
	[Number_of_Adults] [varchar](50) NULL,
	[Number_of_Generations_in_household] [varchar](50) NULL,
	[Adult_ranges_present_in_household] [varchar](50) NULL,
	[Marital_Status] [varchar](50) NULL,
	[Race_Code] [varchar](50) NULL,
	[Presence_of_Children] [varchar](50) NULL,
	[Number_of_Children] [varchar](50) NULL,
	[Childrens_Ages_Age] [varchar](50) NULL,
	[Childrens_age_ranges] [varchar](50) NULL,
	[Dwelling_Type] [varchar](50) NULL,
	[Home_owner_status] [varchar](50) NULL,
	[Length_of_Residence] [varchar](50) NULL,
	[Home_Market_Value] [varchar](50) NULL,
	[Number_of_Vehicles] [varchar](50) NULL,
	[Vehicle_Make_Code] [varchar](50) NULL,
	[Vehicle_Model_Code] [varchar](50) NULL,
	[Vehicle_Year] [varchar](50) NULL,
	[Net_Worth] [varchar](50) NULL,
	[Income] [varchar](50) NULL,
	[Gender_Individual] [varchar](50) NULL,
	[Age_of_individuals] [varchar](50) NULL,
	[Education_Input_Individuals] [varchar](50) NULL,
	[Occupation_Input_Individuals] [varchar](50) NULL,
	[Education_of_1st_Individual] [varchar](50) NULL,
	[Occupation_1st_Individual] [varchar](50) NULL,
	[Age_of_the_2nd_individual] [varchar](50) NULL,
	[Education_of_2nd_Individual] [varchar](50) NULL,
	[Occupation_2nd_Individual] [varchar](50) NULL,
	[Age_of_the_3rd_individual] [varchar](50) NULL,
	[Education_of_3rd_Individual] [varchar](50) NULL,
	[Occupation_3rd_Individual] [varchar](50) NULL,
	[Age_of_4th_Individual] [varchar](50) NULL,
	[Education_of_4th_Individual] [varchar](50) NULL,
	[Occupation_4th_Individual] [varchar](50) NULL,
	[Age_of_5th_Individual] [varchar](50) NULL,
	[Education_of_5th_Individual] [varchar](50) NULL,
	[Occupation_5th_Individual] [varchar](50) NULL,
	[Political_Party_1st_Individual] [varchar](50) NULL,
	[Voter_Party] [varchar](50) NULL,
	[Household_Clusters] [varchar](50) NULL,
	[Insurance_Groups] [varchar](50) NULL,
	[Financial_Groups] [varchar](50) NULL,
	[Green_Living] [varchar](50) NULL
)
DECLARE @CategorySliceTable TABLE(category varchar(100) NULL, Households varchar(100) NULL)
DECLARE @TablesList TABLE(table_name varchar(100) NULL, column_name varchar(100) NULL)
INSERT INTO @TablesList VALUES
	('##PersonsInHouseHold','Persons In HouseHold')
	,('##AdultsInHousehold','Adults In Household')
	,('##Presence_of_Children','Presence of Children')
	,('##Number_of_Children','Number of Children')
	,('##Marital_Status','Marital Status')
	,('##Number_of_Generations_in_household','Number of Generations in household')
	,('##Race_Code','Race Code')
	,('##Dwelling_Type','Dwelling Type')
	,('##Home_owner_status','Home owner status')
	,('##Length_of_Residence','Length of Residence')
	,('##Home_Market_Value','Home Market Value')
	,('##Net_Worth','Net Worth')
	,('##Income','Income')
	,('##Gender_Individual','Gender')
	,('##Age_of_individuals','Age of individuals')
	,('##Education_Input_Individuals','Education Input Individuals')
	,('##ClusterName','ClusterName')
	,('##Insurance_Group','Insurance Group')
	,('##Financial_Groups','Financial Groups')

DECLARE @table_name varchar(100)
DECLARE @column_name varchar(100)

-- Create temp tables
DECLARE tbl_list_cursor CURSOR LOCAL FAST_FORWARD FOR
		SELECT table_name FROM @TablesList
OPEN tbl_list_cursor
FETCH NEXT FROM tbl_list_cursor INTO @table_name
WHILE @@FETCH_STATUS = 0 
BEGIN
	EXEC('IF OBJECT_ID(''tempdb..' + @table_name + ''') IS NOT NULL DROP TABLE ' + @table_name)
	EXEC('CREATE TABLE ' + @table_name + ' (category varchar(200))')
	FETCH NEXT FROM tbl_list_cursor INTO @table_name
END
CLOSE tbl_list_cursor
DEALLOCATE tbl_list_cursor



-- LOOP OVER THE CLUSTERS
DECLARE @cluster int = 0;
WHILE @cluster < @total_clusters
BEGIN
   -- empty slice table
   DELETE FROM @DemodataSlice;

   -- refill it with current cluster devices
   INSERT INTO @DemodataSlice
	SELECT * FROM Demodata 
	WHERE Household_ID IN (
		SELECT HouseholdID FROM HouseHolders
		WHERE (Mso + '-' + DeviceID) IN 
			(SELECT device_id FROM For_Demo_Analysis 
			WHERE total_parts=@parts AND use_time_decay=@use_time_decay AND cluster_number=@cluster AND area=@area)
	);

	DECLARE @item varchar(200)=NULL
	DECLARE @count int=NULL
	DECLARE @query varchar(max)
	DECLARE @where varchar(max) = ''
	DECLARE @insert varchar(max) = ''
	DECLARE @total_devices float(5)=(SELECT COUNT(0) FROM @DemodataSlice)

	DECLARE @cluster_to_print int = @cluster+1;

	DECLARE tbl_list_cursor CURSOR LOCAL FAST_FORWARD FOR
			SELECT table_name FROM @TablesList
	OPEN tbl_list_cursor
	FETCH NEXT FROM tbl_list_cursor INTO @table_name
	WHILE @@FETCH_STATUS = 0 
	BEGIN
		EXEC('ALTER TABLE ' + @table_name + ' ADD [cls ' + @cluster_to_print + '] float(5)')

		DELETE FROM @CategorySliceTable;
		IF @table_name='##PersonsInHouseHold'
		BEGIN
			INSERT INTO @CategorySliceTable 
			SELECT (case when Household_size='' then 'z_Unknown' else Household_size end) as category,count(0) as [Households] 
			FROM @DemodataSlice
			GROUP BY Household_size
		END
		ELSE IF @table_name='##AdultsInHousehold'
		BEGIN
			INSERT INTO @CategorySliceTable 
			SELECT (case when Number_of_Adults='' then  'z_Unknown' else Number_of_Adults end) as category,count(0) as [Households] 
			FROM @DemodataSlice
			GROUP BY Number_of_Adults
		END
		ELSE IF @table_name='##Presence_of_Children'
		BEGIN
			INSERT INTO @CategorySliceTable 
			SELECT (case when Presence_of_Children='' then 'z_Unknown' else Presence_of_Children end) as category,count(0) as [Households] 
			FROM @DemodataSlice
			GROUP BY Presence_of_Children
		END
		ELSE IF @table_name='##Number_of_Children'
		BEGIN
			INSERT INTO @CategorySliceTable 
			SELECT (case when Number_of_Children='' then 'z_Unknown' else Number_of_Children end) as category,count(0) as [Households] 
			FROM @DemodataSlice
			GROUP BY Number_of_Children
		END
		ELSE IF @table_name='##Marital_Status'
		BEGIN
			INSERT INTO @CategorySliceTable 
			SELECT (case when Marital_Status='M' then 'Married' 
			when Marital_Status='S' then 'Single' 
			when Marital_Status='A' then 'Inferred Married'
			when Marital_Status='B' then 'Inferred Single' else 'z_Unknown' end) as category,count(0) as [Households] 
			FROM @DemodataSlice
			GROUP BY Marital_Status
		END
		ELSE IF @table_name='##Number_of_Generations_in_household'
		BEGIN
			INSERT INTO @CategorySliceTable 
			SELECT (case when Number_of_Generations_in_household='' then 'z_Unknown' else Number_of_Generations_in_household end) as category,count(0) as [Households] 
			FROM @DemodataSlice
			GROUP BY Number_of_Generations_in_household
		END
		ELSE IF @table_name='##Race_Code'
		BEGIN
			INSERT INTO @CategorySliceTable 
			SELECT (case WHEN Race_Code='A' THEN 'Asian' 
			WHEN Race_Code='B' THEN 'African American' 
			WHEN Race_Code='H' THEN 'Hispanic'
			WHEN Race_Code='W' THEN 'White/Other' ELSE 'z_Unknown' END) as category,count(0) as [Households] 
			FROM @DemodataSlice
			GROUP BY Race_Code
		END
		ELSE IF @table_name='##Dwelling_Type'
		BEGIN
			INSERT INTO @CategorySliceTable 
			SELECT (case when Dwelling_Type='M' then 'Multiple Family Dwelling Unit' 
			when Dwelling_Type='S' then 'Single Family Dwelling Unit' else 'z_Unknown' end) as category,count(0) as [Households] 
			FROM @DemodataSlice
			GROUP BY Dwelling_Type
		END
		ELSE IF @table_name='##Home_owner_status'
		BEGIN
			INSERT INTO @CategorySliceTable 
			select (case when Home_owner_status='O' then 'Home Owner' 
			when Home_owner_status='R' then 'Renter' else 'z_Unknown' end)  
			as category,count(0) as [Households] from @DemodataSlice
			group by Home_owner_status
		END
		ELSE IF @table_name='##Length_of_Residence'
		BEGIN
			INSERT INTO @CategorySliceTable 
			select Length_of_Residence as category,count(0) as [Households] from @DemodataSlice
			group by Length_of_Residence
		END
		ELSE IF @table_name='##Home_Market_Value'
		BEGIN
			INSERT INTO @CategorySliceTable 
			select (case when Home_Market_Value='A' then '$1,000-$24,999' 
					when Home_Market_Value='B' then '$25,000-$49,999' 
					when Home_Market_Value='C' then '$50,000-$74,999'
					when Home_Market_Value='D' then '$75,000-$99,999'
					when Home_Market_Value='E' then '$100,000-$124,999'
					when Home_Market_Value='F' then '$125,000-$149,999'
					when Home_Market_Value='G' then '$150,000-$174,999'
					when Home_Market_Value='H' then '$175,000-$199,999'
					when Home_Market_Value='I' then '$200,000-$224,999'
					when Home_Market_Value='J' then '$225,000-$249,999'
					when Home_Market_Value='K' then '$250,000-$274,000'
					when Home_Market_Value='L' then '$275,000-$299,999'
					when Home_Market_Value='M' then '$300,000-$349,999'
					when Home_Market_Value='N' then '$350,000-$399,999'
					when Home_Market_Value='O' then '$400,000-$449,999' 
					when Home_Market_Value='P' then '$450,000-$499,999'
					when Home_Market_Value='Q' then '$500,000-$749,999'
					when Home_Market_Value='R' then '$750,000-$999,999'
					when Home_Market_Value='S' then '$1,000,000 Plus' else 'z_Unknown' end)
			as category,count(0) as [Households] from @DemodataSlice
			group by Home_Market_Value
		END
		ELSE IF @table_name='##Net_Worth'
		BEGIN
			INSERT INTO @CategorySliceTable 
			select (case when Net_Worth='1' then 'Less than $1' 
					when Net_Worth='2' then '$1-$4,999' 
					when Net_Worth='3' then '$5,00-$9,999'
					when Net_Worth='4' then '$10,000-$24,999'
					when Net_Worth='5' then '$25,000-$49,999'
					when Net_Worth='6' then '$50,000-$99,999'
					when Net_Worth='7' then '$100,000-$249,000'
					when Net_Worth='8' then '$250,000-$499,999'
					when Net_Worth='9' then 'Greater than $499,999' else 'z_Unknown' end)
			as category,count(0) as [Households] from @DemodataSlice
			group by Net_Worth
		END
		ELSE IF @table_name='##Income'
		BEGIN
			INSERT INTO @CategorySliceTable 
			select (case when Income ='1' then 'Less than $15,000' 
					when Income='2' then '$15,000-$19,999' 
					when Income='3' then '$20,000-$29,999'
					when Income='4' then '$30,000-$39,999'
					when Income='5' then '$40,000-$49,999'
					when Income='6' then '$50,000-$59,999'
					when Income='7' then '$60,000-$69,999'
					when Income='8' then '$70,000-$79,999'
					when Income='9' then '$80,000-$89,999'
					when Income='A' then '$90,000-$99,999'
					when Income='B' then '$100,000-$124,999'
					when Income='C' then '$125,000-$149,999'
					when Income='D' then 'Greater than $149,999' else 'z_Unknown' end)
			as category,count(0) as [Households] from @DemodataSlice
			group by Income
		END
		ELSE IF @table_name='##Gender_Individual'
		BEGIN
			INSERT INTO @CategorySliceTable 
			select (case when Gender_Individual='M' then 'Male' 
			when Gender_Individual='F' then 'Female' else 'z_Unknown' end) 
			as category,count(0) as [Households] from @DemodataSlice
			group by Gender_Individual
		END
		ELSE IF @table_name='##Age_of_individuals'
		BEGIN
			INSERT INTO @CategorySliceTable 
			select (case when Age_of_individuals='17' then 'Age is less than 18'
					when Age_of_individuals='18' then 'Age 18-19'
					when Age_of_individuals='20' then 'Age 20-21'
					when Age_of_individuals='22' then 'Age 22-23'
					when Age_of_individuals='24' then 'Age 24-25'
					when Age_of_individuals='26' then 'Age 26-27'
					when Age_of_individuals='28' then 'Age 28-29'
					when Age_of_individuals='30' then 'Age 30-31'
					when Age_of_individuals='32' then 'Age 32-33'
					when Age_of_individuals='34' then 'Age 34-35'
					when Age_of_individuals='36' then 'Age 36-37'
					when Age_of_individuals='38' then 'Age 38-39'
					when Age_of_individuals='40' then 'Age 40-41'
					when Age_of_individuals='42' then 'Age 42-43'
					when Age_of_individuals='44' then 'Age 44-45'
					when Age_of_individuals='46' then 'Age 46-47'
					when Age_of_individuals='48' then 'Age 48-49'
					when Age_of_individuals='50' then 'Age 50-51'
					when Age_of_individuals='52' then 'Age 52-53'
					when Age_of_individuals='54' then 'Age 54-55'
					when Age_of_individuals='56' then 'Age 56-57'
					when Age_of_individuals='58' then 'Age 58-59'
					when Age_of_individuals='60' then 'Age 59-60'
					when Age_of_individuals='62' then 'Age 62-63'
					when Age_of_individuals='64' then 'Age 64-65'
					when Age_of_individuals='66' then 'Age 66-67'
					when Age_of_individuals='68' then 'Age 68-69'
					when Age_of_individuals='70' then 'Age 70-71'
					when Age_of_individuals='72' then 'Age 72-73'
					when Age_of_individuals='74' then 'Age 74-75'
					when Age_of_individuals='76' then 'Age 76-77'
					when Age_of_individuals='78' then 'Age 78-79'
					when Age_of_individuals='80' then 'Age 80-81'
					when Age_of_individuals='82' then 'Age 82-83'
					when Age_of_individuals='84' then 'Age 84-85'
					when Age_of_individuals='86' then 'Age 86-87'
					when Age_of_individuals='88' then 'Age 88-89'
					when Age_of_individuals='90' then 'Age 90-91'
					when Age_of_individuals='92' then 'Age 92-93'
					when Age_of_individuals='94' then 'Age 94-95'
					when Age_of_individuals='96' then 'Age 96-97'
					when Age_of_individuals='98' then 'Age 98-99'
					when Age_of_individuals='99' then 'Age greater than 99' else 'z_Unknown' end)
			as category,count(0) as [Households] from @DemodataSlice
			group by Age_of_individuals
		END
		ELSE IF @table_name='##Education_Input_Individuals'
		BEGIN
			INSERT INTO @CategorySliceTable 
			select (case when Education_Input_Individuals='1' then 'Completed High School' 
					when Education_Input_Individuals='2' then 'Completed College' 
					when Education_Input_Individuals='3' then 'Completed Graduate School'
					when Education_Input_Individuals='4' then 'Attended Vocational/Technical' else 'z_Unknown' end)
			as category,count(0) as [Households] from @DemodataSlice
			group by Education_Input_Individuals
		END
		ELSE IF @table_name='##ClusterName'
		BEGIN
			INSERT INTO @CategorySliceTable 
			select category, SUM(Households) as Households from 
			(select Household_Clusters,(case when Household_Clusters like '01%' then 'Summit Estates'
					when Household_Clusters like '02%' then 'Established Elite'
					when Household_Clusters like '03%' then 'Corporate Clout'
					when Household_Clusters like '04%' then 'Skyboxes and Suburbans'
					when Household_Clusters like '05%' then 'Sitting Pretty'
					when Household_Clusters like '06%' then 'Shooting Stars'
					when Household_Clusters like '07%' then 'Lavish Lifestyles'
					when Household_Clusters like '08%' then 'Full Steaming'
					when Household_Clusters like '09%' then 'Platinum Oldies'
					when Household_Clusters like '10%' then 'Hard Chargers'
					when Household_Clusters like '11%' then 'Kids and Clout'
					when Household_Clusters like '12%' then 'Tots and Toys'
					when Household_Clusters like '13%' then 'Solid Single Parents'
					when Household_Clusters like '14%' then 'Career-Centered Singles'
					when Household_Clusters like '15%' then 'Country Ways'
					when Household_Clusters like '16%' then 'Country Single'
					when Household_Clusters like '17%' then 'Apple Pie Families'
					when Household_Clusters like '18%' then 'Married Sophisticated'
					when Household_Clusters like '19%' then 'Country Comfort'
					when Household_Clusters like '20%' then 'Dynamic Duos'
					when Household_Clusters like '21%' then 'Children First'
					when Household_Clusters like '22%' then 'Fun and Games'
					when Household_Clusters like '23%' then 'Acres Couples'
					when Household_Clusters like '24%' then 'Career Building'
					when Household_Clusters like '25%' then 'Clubs and Causes'
					when Household_Clusters like '26%' then 'Savvy Singles'
					when Household_Clusters like '27%' then 'Soccer and SUVs'
					when Household_Clusters like '28%' then 'Suburban Seniors'
					when Household_Clusters like '29%' then 'City Mixers'
					when Household_Clusters like '30%' then 'Spouses and Houses'
					when Household_Clusters like '31%' then 'Mid Americana'
					when Household_Clusters like '32%' then 'Metro Mix'
					when Household_Clusters like '33%' then 'Urban Tenants'
					when Household_Clusters like '34%' then 'Outward Bound'
					when Household_Clusters like '35%' then 'Solo and Stable'
					when Household_Clusters like '36%' then 'Raisin’ Grandkids'
					when Household_Clusters like '37%' then 'Cartoons and Carpools'
					when Household_Clusters like '38%' then 'Midtown Minivanners'
					when Household_Clusters like '39%' then 'Early Parents'
					when Household_Clusters like '40%' then 'The Great Outdoors'
					when Household_Clusters like '41%' then 'Truckin’ & Stylin’'
					when Household_Clusters like '42%' then 'First Mortgage'
					when Household_Clusters like '43%' then 'Work & Causes'
					when Household_Clusters like '44%' then 'Community Singles'
					when Household_Clusters like '45%' then 'First Digs'
					when Household_Clusters like '46%' then 'Home Cooking'
					when Household_Clusters like '47%' then 'Rural Parents'
					when Household_Clusters like '48%' then 'Farmland Families'
					when Household_Clusters like '49%' then 'Devoted Duos'
					when Household_Clusters like '50%' then 'Rural Retirement'
					when Household_Clusters like '51%' then 'Family Matters'
					when Household_Clusters like '52%' then 'Resolution Renters'
					when Household_Clusters like '53%' then 'Metro Parents'
					when Household_Clusters like '54%' then 'Still Truckin’'
					when Household_Clusters like '55%' then 'Humble Homes'
					when Household_Clusters like '56%' then 'Modest Wages'
					when Household_Clusters like '57%' then 'Collegiate Crowd'
					when Household_Clusters like '58%' then 'Young Workboots'
					when Household_Clusters like '59%' then 'Mobile Mixers'
					when Household_Clusters like '60%' then 'Rural Rovers'
					when Household_Clusters like '61%' then 'Urban Scramble'
					when Household_Clusters like '62%' then 'Kids and Rent'
					when Household_Clusters like '63%' then 'Downtown Dwellers'
					when Household_Clusters like '64%' then 'Rural Everlasting'
					when Household_Clusters like '65%' then 'Thrifty Elders'
					when Household_Clusters like '66%' then 'Timeless Elders'
					when Household_Clusters like '67%' then 'Rolling Stones'
					when Household_Clusters like '68%' then 'Pennywise Proprietors'
					when Household_Clusters like '69%' then 'Pennywise Mortgages'
					when Household_Clusters like '70%' then 'Resilient Renters' else 'z_Unknown' end)
			as category,count(0) as [Households] from @DemodataSlice
			group by Household_Clusters) as t
			group by t.category
		END
		ELSE IF @table_name='##Insurance_Group'
		BEGIN
			INSERT INTO @CategorySliceTable 
			select category, SUM(Households) as Households from 
			(select Insurance_Groups,(case when Insurance_Groups like '01%' then 'Secured Prosperity'
				when Insurance_Groups like '02%' then 'Stable Singles'
				when Insurance_Groups like '03%' then 'Single Opportunities'
				when Insurance_Groups like '04%' then 'Pennywise Homeowners'
				when Insurance_Groups like '05%' then 'Parenting Priorities'
				when Insurance_Groups like '06%' then 'Comfortable Renters'
				when Insurance_Groups like '07%' then 'Retirement Requirements'
				when Insurance_Groups like '08%' then 'Prosperous Families'
				when Insurance_Groups like '09%' then 'Comfortable Empty'
				when Insurance_Groups like '10%' then 'Modest Maturity'
				when Insurance_Groups like '11%' then 'Modest Country'
				when Insurance_Groups like '12%' then 'Aspiring Affluence'
				when Insurance_Groups like '13%' then 'Pennywise Renters' else 'z_Unknown' end)
			as category,count(0) as [Households] from @DemodataSlice
			group by Insurance_Groups) as t
			group by category
		END
		ELSE IF @table_name='##Financial_Groups'
		BEGIN
			INSERT INTO @CategorySliceTable 
			select t1.[Financial_Groups] as category, SUM(Households) as Households from 
			(select (case when Financial_Groups like '01%' then 'Urban Investors'
				when Financial_Groups like '02%' then 'Suburban Investors'
				when Financial_Groups like '03%' then 'Cautious Planners'
				when Financial_Groups like '04%' then 'Safety First'
				when Financial_Groups like '05%' then 'Savvy Investors'
				when Financial_Groups like '06%' then 'Country Caution'
				when Financial_Groups like '07%' then 'New Market Singles'
				when Financial_Groups like '08%' then 'Cash and Carry Urbanites'
				when Financial_Groups like '09%' then 'Cash and Carry Suburbanites'
				when Financial_Groups like '10%' then 'Rural Security '
				when Financial_Groups like '11%' then 'Getting Started'
				when Financial_Groups like '12%' then 'Financially Challenged ' else 'Unknown' end)
			as Financial_Groups,count(0) as [Households] from @DemodataSlice
			group by Financial_Groups) as t1
			group by t1.Financial_Groups
		END

		DECLARE _cursor CURSOR LOCAL FAST_FORWARD FOR
		SELECT category,Households FROM @CategorySliceTable
		OPEN _cursor
		FETCH NEXT FROM _cursor INTO @item,@count
		WHILE @@FETCH_STATUS = 0 BEGIN
			SET @query = 'IF EXISTS(SELECT 1 FROM ' + @table_name + ' WHERE ' + (CASE WHEN @item IS NULL THEN 'category IS NULL' ELSE 'category=''' + @item + '''' END) + ')
				BEGIN
					UPDATE ' + @table_name + ' SET [cls ' + CONVERT(varchar(max),@cluster_to_print) + '] = ' + CONVERT(varchar(100),@count/@total_devices) + 
					' WHERE ' + (CASE WHEN @item IS NULL THEN 'category IS NULL' ELSE 'category=''' + @item + '''' END) + '
				END
				ELSE
				BEGIN
					INSERT INTO ' + @table_name + ' (category,[cls ' + CONVERT(varchar(max),@cluster_to_print) + ']) VALUES ('''+ @item + ''',' + CONVERT(varchar(100),@count/@total_devices) + ')
				END'
			--SELECT @query
			EXEC(@query)
			FETCH NEXT FROM _cursor INTO @item,@count
		END
		CLOSE _cursor
		DEALLOCATE _cursor

		FETCH NEXT FROM tbl_list_cursor INTO @table_name
	END

	CLOSE tbl_list_cursor
	DEALLOCATE tbl_list_cursor


	SET @cluster = @cluster + 1;
END;


-- Print tables

DECLARE tbl_list_cursor CURSOR LOCAL FAST_FORWARD FOR
			SELECT table_name,column_name FROM @TablesList
OPEN tbl_list_cursor
FETCH NEXT FROM tbl_list_cursor INTO @table_name,@column_name
WHILE @@FETCH_STATUS = 0 
BEGIN
	DECLARE @tableName nvarchar(100)
	SET @tableName = @table_name + '.category'
	EXEC tempdb.sys.sp_rename @tableName, @column_name, 'COLUMN';  
	SET @query = 'IF OBJECT_ID(''tempdb..' + @table_name + ''') IS NOT NULL
	BEGIN
		SELECT * FROM ' + @table_name +
		' ORDER BY [' + @column_name + ']
		DROP TABLE ' + @table_name +
	' END'
	EXEC(@query)
	FETCH NEXT FROM tbl_list_cursor INTO @table_name,@column_name
END
CLOSE tbl_list_cursor
DEALLOCATE tbl_list_cursor