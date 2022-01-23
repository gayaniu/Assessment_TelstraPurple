/****** Object:  Schema [dw]    Script Date: 23/01/2022 11:47:23 AM ******/
CREATE SCHEMA [dw]
GO
/****** Object:  Schema [tempstage]    Script Date: 23/01/2022 11:47:23 AM ******/
CREATE SCHEMA [tempstage]
GO
/****** Object:  Table [dbo].[DW_Ingestion_Errors]    Script Date: 23/01/2022 11:47:23 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DW_Ingestion_Errors](
	[ErrorID] [int] IDENTITY(1,1) NOT NULL,
	[TableName] [varchar](100) NULL,
	[UserName] [varchar](100) NULL,
	[ErrorNumber] [int] NULL,
	[ErrorState] [int] NULL,
	[ErrorSeverity] [int] NULL,
	[ErrorLine] [int] NULL,
	[ErrorProcedure] [varchar](max) NULL,
	[ErrorMessage] [varchar](max) NULL,
	[ErrorDateTime] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dw].[DmBarnd]    Script Date: 23/01/2022 11:47:23 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dw].[DmBarnd](
	[BrandId] [int] IDENTITY(1,1) NOT NULL,
	[BrandName] [nvarchar](max) NULL,
 CONSTRAINT [IDX_DmBrand_BrandId] PRIMARY KEY CLUSTERED 
(
	[BrandId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dw].[DmSite]    Script Date: 23/01/2022 11:47:23 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dw].[DmSite](
	[SiteID] [int] IDENTITY(1,1) NOT NULL,
	[TradingName] [nvarchar](max) NULL,
	[Location] [nvarchar](max) NULL,
	[Address] [nvarchar](max) NULL,
	[Phone] [nvarchar](max) NULL,
	[Latitude] [float] NULL,
	[Longitude] [float] NULL,
	[FullAddress]  AS (([Address]+', ')+[Location]),
 CONSTRAINT [IDX_DmSite_SiteID] PRIMARY KEY CLUSTERED 
(
	[SiteID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dw].[DmSiteFeatures]    Script Date: 23/01/2022 11:47:23 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dw].[DmSiteFeatures](
	[SiteFeatureID] [int] IDENTITY(1,1) NOT NULL,
	[SiteID] [int] NULL,
	[Trading_period] [nvarchar](max) NULL,
	[ATM_Avaialble] [nvarchar](max) NULL,
	[Paymethod] [nvarchar](max) NULL,
	[Driveway_Services] [nvarchar](max) NULL,
	[Station] [nvarchar](max) NULL,
	[Other_Features] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dw].[Fact_FuelPrice]    Script Date: 23/01/2022 11:47:23 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dw].[Fact_FuelPrice](
	[FuelPriceID] [int] IDENTITY(1,1) NOT NULL,
	[BrandID] [int] NULL,
	[SiteID] [int] NULL,
	[DateID] [int] NOT NULL,
	[Price] [float] NULL,
	[DateCreated] [datetime] NULL,
	[DateModified] [datetime] NULL,
 CONSTRAINT [IDX_Fact_FuelPrice_DateID] PRIMARY KEY CLUSTERED 
(
	[DateID] ASC,
	[FuelPriceID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [tempstage].[FuelPrices]    Script Date: 23/01/2022 11:47:23 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [tempstage].[FuelPrices](
	[address] [nvarchar](max) NULL,
	[brand] [nvarchar](max) NULL,
	[date] [nvarchar](max) NULL,
	[description] [nvarchar](max) NULL,
	[latitude] [float] NULL,
	[location] [nvarchar](max) NULL,
	[longitude] [float] NULL,
	[phone] [nvarchar](max) NULL,
	[price] [float] NULL,
	[sitefeatures] [nvarchar](max) NULL,
	[title] [nvarchar](max) NULL,
	[tradingname] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE [dw].[Fact_FuelPrice] ADD  DEFAULT (getdate()) FOR [DateCreated]
GO
/****** Object:  StoredProcedure [dw].[SP_Populate_DIM_SiteFeatures]    Script Date: 23/01/2022 11:47:23 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ==================================================================
-- Author: Gayani Udawatta
-- Create Date: 20-01-2021
-- Description: Stored Procedure to Populate Dimension DIM_SiteFeatures
-- ==================================================================
CREATE PROCEDURE [dw].[SP_Populate_DIM_SiteFeatures] 
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
	BEGIN TRY
			DECLARE @t TABLE
			(
			  [tradingname] VARCHAR(MAX) ,
			  sitefeatures VARCHAR(MAX)
			)


			INSERT  INTO @t 
			SELECT [tradingname], [sitefeatures] FROM   [tempstage].[FuelPrices] 

			;WITH    cte
					  AS ( SELECT   [tradingname] ,
									Split.a.value('.', 'VARCHAR(100)') AS ItemTag ,
									ROW_NUMBER() OVER ( PARTITION BY  [tradingname] ORDER BY ( SELECT
																		  NULL
																		  ) ) rn
						   FROM     ( SELECT    [tradingname] ,
												CAST ('<M>' + REPLACE(sitefeatures, ',',
																	  '</M><M>') + '</M>' AS XML) AS ItemTag
									  FROM      @t
									) AS A
									CROSS APPLY ItemTag.nodes('/M') AS Split ( a )
						 )

			SELECT 
				*
				,CASE WHEN LTRIM(A.ItemTag) like  'Open%' THEN 'Trading_period'
					WHEN ( LTRIM(A.ItemTag) like 'Sat%'  OR LTRIM(A.ItemTag) like 'Sun%' OR LTRIM(A.ItemTag) like 'Mon%' OR LTRIM(A.ItemTag) like 'Tue%' OR LTRIM(A.ItemTag) like 'Wed%' OR LTRIM(A.ItemTag) like 'Thu%' OR  LTRIM(A.ItemTag) like 'Fri%' ) 
						  THEN 'Trading_period'	 
					WHEN LTRIM(A.ItemTag) like '%ATM' THEN  'Paymethod' 
					WHEN LTRIM(A.ItemTag) like '%EFTPOS' THEN  'Paymethod' 
					WHEN LTRIM(A.ItemTag) like  '%Driveway%' THEN 'Driveway_Services'
					WHEN LTRIM(A.ItemTag) like  '%Station%'  THEN 'Station'
					ELSE 'Other_Features'
				END AS [identifier]
			INTO #Temp_Table
			FROM cte AS A


			SELECT 
				[tradingname]
				, STRING_AGG(ItemTag, ', ') AS [item_Tag_]
			INTO #Temp_Table_SiteFeatures
			FROM #Temp_Table A
			WHERE ItemTag <> ''
			GROUP BY [tradingname],identifier
			ORDER BY [tradingname]

			SELECT * 
			,CASE WHEN LTRIM(A.item_Tag_) like  'Open%' THEN 'Trading_period' 
				WHEN LTRIM(A.item_Tag_) like 'ATM%' THEN  'ATM_Avaialble' 
				WHEN LTRIM(A.item_Tag_) like '%EFTPOS%' THEN  'Paymethod' 
				WHEN LTRIM(A.item_Tag_) like  '%Driveway%' THEN 'Driveway_Services'
				WHEN LTRIM(A.item_Tag_) like  '%Station%'  THEN 'Station'
				ELSE 'Other_Features'
			END AS [identifier]
			INTO #TEMP_Consolidated_SiteFeatures
			FROM #Temp_Table_SiteFeatures A
			ORDER BY [tradingname]

			TRUNCATE TABLE [dw].[DmSiteFeatures]

			INSERT INTO [dw].[DmSiteFeatures]  ([SiteID] ,[Trading_period],[ATM_Avaialble] ,[Paymethod] ,[Driveway_Services] ,[Station] ,[Other_Features]  )
				SELECT 
					(SELECT top 1 [SiteID] FROM [dw].[DmSite] WHERE TradingName = tradingname) AS SiteID
					, (SELECT SF.item_Tag_ FROM #TEMP_Consolidated_SiteFeatures SF WHERE SF.tradingname  = S.tradingname AND SF.identifier ='Trading_period' ) AS [Trading_period]
					, (SELECT SF.item_Tag_ FROM #TEMP_Consolidated_SiteFeatures SF WHERE SF.tradingname  = S.tradingname AND SF.identifier ='ATM_Avaialble'  ) AS ATM_Avaialble
					, (SELECT SF.item_Tag_ FROM #TEMP_Consolidated_SiteFeatures SF WHERE SF.tradingname  = S.tradingname AND SF.identifier ='Paymethod' ) AS Paymethod
					, (SELECT SF.item_Tag_ FROM #TEMP_Consolidated_SiteFeatures SF WHERE SF.tradingname  = S.tradingname AND SF.identifier ='Driveway_Services') AS [Driveway_Services]
					, (SELECT SF.item_Tag_ FROM #TEMP_Consolidated_SiteFeatures SF WHERE SF.tradingname  = S.tradingname AND SF.identifier ='Station' ) AS Station
					, (SELECT SF.item_Tag_ FROM #TEMP_Consolidated_SiteFeatures SF WHERE SF.tradingname  = S.tradingname AND SF.identifier ='Other_Features') AS Other_Features
			FROM [tempstage].[FuelPrices] S 

			DROP TABLE #Temp_Table
			DROP TABLE #Temp_Table_SiteFeatures
			DROP TABLE #TEMP_Consolidated_SiteFeatures
	END TRY
	BEGIN CATCH
	   INSERT INTO  dbo.DW_Ingestion_Errors VALUES
			  (	SUSER_SNAME(),
			    '[dw].[DmSiteFeatures]',
				ERROR_NUMBER(),
				ERROR_STATE(),
				ERROR_SEVERITY(),
				ERROR_LINE(),
				ERROR_PROCEDURE(),
				ERROR_MESSAGE(),
				GETDATE());
	END CATCH;   

END
GO
/****** Object:  StoredProcedure [dw].[SP_Populate_DIM_Tables]    Script Date: 23/01/2022 11:47:23 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ==================================================================
-- Author: Gayani Udawatta
-- Create Date: 20-01-2021
-- Description: Stored Procedure to Populate Dimension tables
-- ==================================================================
CREATE PROCEDURE [dw].[SP_Populate_DIM_Tables] 
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
	BEGIN TRY
		--populating dimension table
		TRUNCATE TABLE  [dw].[DmBarnd] 
		INSERT INTO [dw].[DmBarnd] (BrandName)  
		SELECT brand FROM [tempstage].[FuelPrices]

		TRUNCATE TABLE [dw].[DmSite]
		INSERT INTO [dw].[DmSite] (TradingName ,[Location] ,[Address] ,[Phone]  ,[Latitude], [Longitude] )
		SELECT tradingname, location, address, phone, latitude, longitude FROM [tempstage].[FuelPrices]

	END TRY
	BEGIN CATCH
	   INSERT INTO  dbo.DW_Ingestion_Errors VALUES
			  (	SUSER_SNAME(),
			    '[dw].[SP_Populate_DIM_Tables]',
				ERROR_NUMBER(),
				ERROR_STATE(),
				ERROR_SEVERITY(),
				ERROR_LINE(),
				ERROR_PROCEDURE(),
				ERROR_MESSAGE(),
				GETDATE());
	END CATCH;  

END
GO

/****** Object:  StoredProcedure [dw].[SP_Populate_FACT]    Script Date: 23/01/2022 11:47:23 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ==================================================================
-- Author: Gayani Udawatta
-- Create Date: 20-01-2021
-- Description: Stored Procedure to Populate [Fact_FuelPrice] table
-- ==================================================================
CREATE PROCEDURE [dw].[SP_Populate_FACT] 
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

	BEGIN TRY

		TRUNCATE TABLE [dw].[Fact_FuelPrice]

		INSERT INTO [dw].[Fact_FuelPrice] (BrandID ,SiteID ,DateID ,Price , DateModified)
		SELECT 
			(SELECT top 1 BrandId FROM [dw].[DmBarnd] WHERE brandName = brand) AS [BrandID]
			,(SELECT top 1 SiteID FROM [dw].[DmSite] WHERE TradingName = tradingname) AS SiteID
			,( YEAR([date])*100 + MONTH([date])  ) *100 + DAY([date]) AS DateId
			,price
			, NULL as DateModified
		FROM  [tempstage].[FuelPrices]
		
	END TRY
	BEGIN CATCH
	   INSERT INTO  dbo.DW_Ingestion_Errors VALUES
			  (	SUSER_SNAME(),
			    '[dw].[Fact_FuelPrice]',
				ERROR_NUMBER(),
				ERROR_STATE(),
				ERROR_SEVERITY(),
				ERROR_LINE(),
				ERROR_PROCEDURE(),
				ERROR_MESSAGE(),
				GETDATE());
	END CATCH;   

END
GO
/****** Object:  StoredProcedure [dw].[SP_Populate_DW_Warehouse]    Script Date: 23/01/2022 11:47:23 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ==================================================================
-- Author: Gayani Udawatta
-- Create Date: 20-01-2021
-- Description: Stored Procedure to Insert Data to the Data warehouse
-- ==================================================================
CREATE PROCEDURE [dw].[SP_Populate_DW_Warehouse] 
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
	BEGIN TRY	
		--Populating DIM Tables
		EXEC [dw].[SP_Populate_DIM_Tables]
		EXEC [dw].[SP_Populate_DIM_SiteFeatures]

		--Populating FACT Table
		EXEC [dw].[SP_Populate_FACT]
	END TRY
	BEGIN CATCH
	   INSERT INTO  dbo.DW_Ingestion_Errors VALUES
			  (	SUSER_SNAME(),
			    '[dw].[SP_Populate_DW_Warehouse]',
				ERROR_NUMBER(),
				ERROR_STATE(),
				ERROR_SEVERITY(),
				ERROR_LINE(),
				ERROR_PROCEDURE(),
				ERROR_MESSAGE(),
				GETDATE());
	END CATCH;  
END
GO