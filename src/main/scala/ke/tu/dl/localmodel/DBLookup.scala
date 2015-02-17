package ke.tu.dl.localmodel

object DBLookup {
  import scalikejdbc._
  import ke.tu.dl.utils.ProductionDBConn._

  /**
   * Creates and returns a data source ID
   */
  def createDataSourceId(subscriberId: Int, submissionDate: String): Int = {
    using(DB(getDBConn)) { db =>
      db localTx { implicit session =>
        0
      }
    }
  }

  /**
   * Looks up account status id. If an account status id does not exist,
   * 	it's created and the generated id is returned
   */
  def lookupAccountStatusId(accountStatus: String): Int = {
    using(DB(getDBConn)) { db =>
      db localTx { implicit session =>
        var statusId: Option[Int] = None

        try
          statusId = sql"""
        select AccountStatus_ID from _AccountsStatus 
        where AccountStatus_Name = ${accountStatus}
        """.map(rs => rs.int("AccountStatus_ID")).first.apply
        catch { case e: Exception => }

        if (statusId != None) statusId.getOrElse(0)
        else {
          val statusId = sql"""
          	insert into _AccountsStatus (AccountStatus_Name, AccountStatus_Code) 
          	values(${accountStatus}, ${System.nanoTime() / 1000000})
          """
            .updateAndReturnGeneratedKey.apply
          return statusId.toInt
        }
      }
    }
  }

  /**
   * Looks up account product type. If an account product type does not exist,
   * 	it's created and the new id returned
   */
  def lookupAccountTypeId(accountType: String): Int = {
    using(DB(getDBConn)) { db =>
      db localTx { implicit session =>
        val typeId: Option[Int] = sql"""
        select AccountType_Code from _AccountsType 
        where AccountType_Name = ${accountType}
        """.map(rs => rs.int("AccountType_Code")).first.apply

        if (typeId != None) typeId.getOrElse(0)
        else {
          // Get current max id
          /*
          var nextTypeId = 1
          try {
            val maxId = sql"""
        		  select max(AccountType_Code) as typeId FROM _AccountsType
          """.map(rs => rs.int("typeId")).first.apply
            nextTypeId = maxId.getOrElse(0) + 1
            
          } catch { case e: Exception => }
          */

          // Create the new type with the new id
          val nextTypeId = sql"""
          	insert into _AccountsType (AccountType_Name) 
          		values ( ${accountType})
          """.updateAndReturnGeneratedKey.apply

          return nextTypeId.toInt
        }
      }
    }
  }

  /**
   * Looks up account holder type id. If the an account holder type id does not
   * 	exist, it's created and the generated id is returned
   */
  def lookupAccountHolderTypeId(accountHolderType: String): Int = {
    using(DB(getDBConn)) { db =>
      db localTx { implicit session =>
        val ownerId: Option[Int] = sql"""
        select AccountOwner_Code from _AccountsOwner 
        where AccountOwner_Name = ${accountHolderType}
        """.map(rs => rs.int("AccountOwner_Code")).first.apply

        if (ownerId != None) ownerId.getOrElse(0)
        else {
          // Get current max id
          /*
          var nextOwnerId = 1
          try {
            val maxId = sql"""
        		  select max(AccountOwner_Code) as ownerId FROM _AccountsOwner
          """.map(rs => rs.int("ownerId")).first.apply
            nextOwnerId = maxId.getOrElse(0) + 1
          } catch { case e: Exception => }
          */

          // Create the new type with the new id
          val nextOwnerId = sql"""
          	insert into _AccountsOwner (AccountOwner_Name) 
          		values (${accountHolderType})
          """.updateAndReturnGeneratedKey.apply

          return nextOwnerId.toInt
        }
      }
    }
  }

  /**
   * Lookup sector id. If doesn't exist, create and return the generated id
   */
  def lookupSectorId(sectorName: String): Long = {
    using(DB(getDBConn)) { db =>
      db localTx { implicit session =>
        val sectorId = sql"""
        	select Sector_ID from _Sectors where Sector_Name = ${sectorName}
        """.map(rs => rs.long("Sector_ID")).first.apply

        if (sectorId != None) sectorId.getOrElse(0)
        else {
          val sectorId = sql"""
        			insert into _Sectors (Sector_Name, Sector_DateCreated) 
        				values(${sectorName}, ${new java.sql.Date(new java.util.Date().getTime)})
        	""".updateAndReturnGeneratedKey.apply

          sectorId.toLong
        }
      }
    }
  }

  /**
   * Looks up currency id. Creates if doesn't exist and returns the generated id
   */
  def lookupCurrencyId(currencyName: String): Int = {
    using(DB(getDBConn)) { db =>
      db localTx { implicit session =>
        val currencyId = sql"""
      			select Currency_ID from _Currencies where Currency_Name = ${currencyName}
      		""".map(rs => rs.int("Currency_ID")).first.apply

        if (currencyId != None) currencyId.getOrElse(0)
        else {
          val currencyId = sql"""
      				  insert into _Currencies (Currency_Name) values (${currencyName})
      		  """
            .updateAndReturnGeneratedKey.apply

          currencyId.toInt
        }
      }
    }
  }

  /**
   * Looks up repayment term. If it doesn't exist, creates and returns the generated ID
   */
  def lookupRepaymentTerm(termName: String): Int = {
    using(DB(getDBConn)) { db =>
      db localTx { implicit session =>
        val termId = sql"""
      		select AccountRepayTerm_Code from _AccountsRepaymentTerm
      			where AccountRepayTerm_Name = ${termName}
      	""".map(rs => rs.int("AccountRepayTerm_Code")).first.apply

        if (termId != None) termId.getOrElse(0)
        else {
          /*
          var nextTermId = 1
          try {
            val maxId = sql"""
      			  select max(AccountRepayTerm_Code) as maxId from _AccountsRepaymentTerm
      	  """.map(rs => rs.int("maxId")).single.apply
            nextTermId = maxId.getOrElse(0) + 1
          } catch { case e: Exception => }
          */

          val termId = sql"""
      			 insert into _AccountsRepaymentTerm (AccountRepayTerm_Name) 
      			  	values(${termName})
      	  """.updateAndReturnGeneratedKey.apply

          termId.toInt
        }
      }
    }
  }

  /**
   * Looks up nationality id. Creates the ID if not existing
   */
  def lookupNationalityId(nationalityName: String): Int = {
    using(DB(getDBConn)) { db =>
      db localTx { implicit session =>
        val nationalityId = sql"""
      		select Nationality_ID as nationalityId from _Nationalities 
        	where Nationality_Name = ${nationalityName.trim}
      	""".map(rs => rs.int("nationalityId")).first.apply

        if (nationalityId != None) nationalityId.get
        else {
          val nationalityId = sql"""
      			  insert into _Nationalities (Nationality_Name, Nationality_DateCreated) 
      			  values (${nationalityName}, ${new java.sql.Date(new java.util.Date().getTime)} )
      	  """
            .updateAndReturnGeneratedKey.apply

          nationalityId.toInt
        }

      }
    }
  }
}