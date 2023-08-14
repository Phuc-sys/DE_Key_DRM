DROP SCHEMA IF EXISTS Key_DRM;

CREATE SCHEMA Key_DRM;

CREATE TABLE IF NOT EXISTS Key_DRM.Customers(         
	CustomerID varchar(255) PRIMARY KEY,
	Mac varchar(255),
	Created_date varchar(255)
);

CREATE TABLE IF NOT EXISTS Key_DRM.CustomerService(    
	CustomerID varchar(255) PRIMARY KEY,
	ServiceID varchar(255),
	Amount int,
	Date varchar(255)
);

CREATE TABLE IF NOT EXISTS Key_DRM.Log_Get_DRM_List(        
	CustomerID varchar(255) PRIMARY KEY,
	Date varchar(255),
	Mac varchar(255)
);

CREATE TABLE IF NOT EXISTS Key_DRM.Log_BHD_MovieID(    
	CustomerID varchar(255),
	MovieID varchar(255),
	RealTimePlaying int,
	Date varchar(255)
);

CREATE TABLE IF NOT EXISTS Key_DRM.Log_Fimplus_MovieID(  
	CustomerID varchar(255),
	MovieID varchar(255),
	RealTimePlaying int,
	Date varchar(255)
);

CREATE TABLE IF NOT EXISTS Key_DRM.MV_PropertiesShowVN(    
	ID varchar(255) PRIMARY KEY,
	TopTitle varchar(255),
	TitleEN varchar(255),
	Release varchar(4),
	Actors varchar(255),
	Directors varchar(255),
	Producers varchar(255),
	PublishCountry varchar(255),
	Duration int,
	isDRM bool
);

