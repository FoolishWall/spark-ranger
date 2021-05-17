/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive


import org.apache.commons.logging.LogFactory
import org.apache.ranger.authorization.spark.authorizer.{SparkPrivObjectActionType, SparkPrivilegeObject, SparkPrivilegeObjectType}
import org.apache.ranger.authorization.spark.authorizer.SparkPrivObjectActionType.SparkPrivObjectActionType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.AuthzUtils._
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.sql.types.StructField

import scala.collection.mutable.ArrayBuffer

/**
 * [[LogicalPlan]] -> list of [[SparkPrivilegeObject]]s
 */
private[sql] object PrivilegesBuilder {

  private val LOG = LogFactory.getLog(this.getClass.getSimpleName.stripSuffix("$"))

  /**
   * Build input and output privilege objects from a Spark's [[LogicalPlan]]
   *
   * For [[ExplainCommand]]s, build its child.
   * For [[RunnableCommand]]s, build outputs if it has an target to write, build inputs for the
   * inside query if exists.
   *
   * For other queries, build inputs.
   *
   * @param plan A Spark [[LogicalPlan]]
   */
  def build(plan: LogicalPlan, spark: SparkSession): (Seq[SparkPrivilegeObject], Seq[SparkPrivilegeObject]) = {

    def doBuild(plan: LogicalPlan, spark: SparkSession): (Seq[SparkPrivilegeObject], Seq[SparkPrivilegeObject]) = {
      val inputObjs = new ArrayBuffer[SparkPrivilegeObject]
      val outputObjs = new ArrayBuffer[SparkPrivilegeObject]
      plan match {
        // RunnableCommand
        case cmd: Command => buildCommand(cmd, inputObjs, outputObjs, spark)
        // Queries
        case _ => buildQuery(plan, inputObjs)
      }
      LOG.info("***inputObjs***" + inputObjs)
      LOG.info("***outputObjs***" + outputObjs)
      (inputObjs, outputObjs)
    }

    plan match {
      case e: ExplainCommand => doBuild(e.logicalPlan, spark)
      case p => doBuild(p, spark)
    }
  }

  /**
   * Build SparkPrivilegeObjects from Spark LogicalPlan
   *
   * @param plan             a Spark LogicalPlan used to generate SparkPrivilegeObjects
   * @param privilegeObjects input or output spark privilege object list
   * @param projectionList   Projection list after pruning
   */
  private def buildQuery(
                            plan: LogicalPlan,
                            privilegeObjects: ArrayBuffer[SparkPrivilegeObject],
                            projectionList: Seq[NamedExpression] = Nil): Unit = {

    LOG.info("*** projectionList ***" + projectionList)

    /**
     * Columns in Projection take priority for column level privilege checking
     *
     * @param table catalogTable of a given relation
     */
    def mergeProjection(table: CatalogTable): Unit = {
      LOG.info("*** CatalogTable: table ***" + table)
      if (projectionList.isEmpty) {
        addTableOrViewLevelObjs(
          table.identifier,
          privilegeObjects,
          table.partitionColumnNames,
          table.schema.fieldNames)
      } else {
        addTableOrViewLevelObjs(
          table.identifier,
          privilegeObjects,
          table.partitionColumnNames.filter(projectionList.map(_.name).contains(_)),
          projectionList.map(_.name))
      }
    }

    plan match {
      case p: Project => buildQuery(p.child, privilegeObjects, p.projectList)

      case h if h.nodeName == "HiveTableRelation" =>
        mergeProjection(getFieldVal(h, "tableMeta").asInstanceOf[CatalogTable])

      case m if m.nodeName == "MetastoreRelation" =>
        mergeProjection(getFieldVal(m, "catalogTable").asInstanceOf[CatalogTable])

      case l: LogicalRelation if l.catalogTable.nonEmpty => mergeProjection(l.catalogTable.get)

      case u: UnresolvedRelation =>
        // Normally, we shouldn't meet UnresolvedRelation here in an optimized plan.
        // Unfortunately, the real world is always a place where miracles happen.
        // We check the privileges directly without resolving the plan and leave everything
        // to spark to do.
        addTableOrViewLevelObjs(u.tableIdentifier, privilegeObjects)

      case p =>
        for (child <- p.children) {
          buildQuery(child, privilegeObjects, projectionList)
        }
    }
  }

  /**
   * Build SparkPrivilegeObjects from Spark LogicalPlan
   *
   * @param plan       a Spark LogicalPlan used to generate SparkPrivilegeObjects
   * @param inputObjs  input spark privilege object list
   * @param outputObjs output spark privilege object list
   */
  private def buildCommand(
                              plan: LogicalPlan,
                              inputObjs: ArrayBuffer[SparkPrivilegeObject],
                              outputObjs: ArrayBuffer[SparkPrivilegeObject],
                              spark: SparkSession): Unit = {
    plan match {
      case a: AlterDatabasePropertiesCommand => addDbLevelObjs(a.databaseName, outputObjs)

      case a if a.nodeName == "AlterTableAddColumnsCommand" =>
        //add database info
        val tableIdentifier = getFieldVal(a, "table").asInstanceOf[TableIdentifier]
        addTableOrViewLevelObjs(
          tableIdentifier.copy(tableIdentifier.table, Some(spark.catalog.currentDatabase)),
          inputObjs,
          columns = getFieldVal(a, "colsToAdd").asInstanceOf[Seq[StructField]].map(_.name))
        addTableOrViewLevelObjs(
          tableIdentifier.copy(tableIdentifier.table, Some(spark.catalog.currentDatabase)),
          outputObjs,
          columns = getFieldVal(a, "colsToAdd").asInstanceOf[Seq[StructField]].map(_.name))

      case a: AlterTableAddPartitionCommand =>
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), inputObjs)
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), outputObjs)

      case a if a.nodeName == "AlterTableChangeColumnCommand" =>
        val tableIdentifier = getFieldVal(a, "table").asInstanceOf[TableIdentifier]
        addTableOrViewLevelObjs(
          tableIdentifier.copy(tableIdentifier.table, Some(spark.catalog.currentDatabase)),
          inputObjs,
          columns = Seq(getFieldVal(a, "columnName").asInstanceOf[String]))

      case a: AlterTableDropPartitionCommand =>
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), inputObjs)
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), outputObjs)

      case a: AlterTableRecoverPartitionsCommand =>
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), inputObjs)
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), outputObjs)

      case a: AlterTableRenameCommand if !a.isView || a.oldName.database.nonEmpty =>
        // rename tables / permanent views
        addTableOrViewLevelObjs(a.oldName.copy(a.oldName.table, Some(spark.catalog.currentDatabase)), inputObjs)
        addTableOrViewLevelObjs(a.newName.copy(a.newName.table, Some(spark.catalog.currentDatabase)), outputObjs)

      case a: AlterTableRenamePartitionCommand =>
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), inputObjs)
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), outputObjs)

      case a: AlterTableSerDePropertiesCommand =>
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), inputObjs)
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), outputObjs)

      case a: AlterTableSetLocationCommand =>
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), inputObjs)
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), outputObjs)

      case a: AlterTableSetPropertiesCommand =>
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), inputObjs)
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), outputObjs)

      case a: AlterTableUnsetPropertiesCommand =>
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), inputObjs)
        addTableOrViewLevelObjs(a.tableName.copy(a.tableName.table, Some(spark.catalog.currentDatabase)), outputObjs)

      case a: AlterViewAsCommand =>
        if (a.name.database.nonEmpty) {
          // it's a permanent view
          addTableOrViewLevelObjs(a.name.copy(a.name.table, Some(spark.catalog.currentDatabase)), outputObjs)
        }
        buildQuery(a.query, inputObjs)

      case a: AnalyzeColumnCommand =>
        addTableOrViewLevelObjs(
          a.tableIdent, inputObjs, columns = a.columnNames)
        addTableOrViewLevelObjs(
          a.tableIdent, outputObjs, columns = a.columnNames)

      case a if a.nodeName == "AnalyzePartitionCommand" =>
        addTableOrViewLevelObjs(
          getFieldVal(a, "tableIdent").asInstanceOf[TableIdentifier], inputObjs)
        addTableOrViewLevelObjs(
          getFieldVal(a, "tableIdent").asInstanceOf[TableIdentifier], outputObjs)

      case a: AnalyzeTableCommand =>
        addTableOrViewLevelObjs(a.tableIdent, inputObjs, columns = Seq("RAW__DATA__SIZE"))
        addTableOrViewLevelObjs(a.tableIdent, outputObjs)

      case c: CacheTableCommand => c.plan.foreach {
        buildQuery(_, inputObjs)
      }

      case c: CreateDatabaseCommand => addDbLevelObjs(c.databaseName, outputObjs)

      case c: CreateDataSourceTableAsSelectCommand =>
        addDbLevelObjs(c.table.identifier, outputObjs)
        addTableOrViewLevelObjs(c.table.identifier, outputObjs, mode = c.mode)
        buildQuery(c.query, inputObjs)

      case c: CreateDataSourceTableCommand =>
        addTableOrViewLevelObjs(c.table.identifier, outputObjs)

      case c: CreateFunctionCommand if !c.isTemp =>
        addDbLevelObjs(c.databaseName, outputObjs)
        addFunctionLevelObjs(c.databaseName, c.functionName, outputObjs)

      case c: CreateHiveTableAsSelectCommand =>
        addDbLevelObjs(c.tableDesc.identifier, outputObjs)
        addTableOrViewLevelObjs(c.tableDesc.identifier, outputObjs)
        buildQuery(c.query, inputObjs)

      case c: CreateTableCommand => addTableOrViewLevelObjs(c.table.identifier, outputObjs)

      case c: CreateTableLikeCommand =>
        addDbLevelObjs(c.targetTable, outputObjs)
        addTableOrViewLevelObjs(c.targetTable, outputObjs)
        // hive don't handle source table's privileges, we should not obey that, because
        // it will cause meta information leak
        addDbLevelObjs(c.sourceTable, inputObjs)
        addTableOrViewLevelObjs(c.sourceTable, inputObjs)

      case c: CreateViewCommand =>
        c.viewType match {
          case PersistedView =>
            // PersistedView will be tied to a database
            addDbLevelObjs(c.name, outputObjs)
            addTableOrViewLevelObjs(c.name, outputObjs)
          case _ =>
        }
        buildQuery(c.child, inputObjs)

      case d if d.nodeName == "DescribeColumnCommand" =>
        addTableOrViewLevelObjs(
          getFieldVal(d, "table").asInstanceOf[TableIdentifier],
          inputObjs,
          columns = getFieldVal(d, "colNameParts").asInstanceOf[Seq[String]])

      case d: DescribeDatabaseCommand =>
        addDbLevelObjs(d.databaseName, inputObjs)

      case d: DescribeFunctionCommand =>
        addFunctionLevelObjs(d.functionName.database, d.functionName.funcName, inputObjs)

      case d: DescribeTableCommand =>
        addTableOrViewLevelObjs(d.table.copy(d.table.table, Some(spark.catalog.currentDatabase)), inputObjs)

      case d: DropDatabaseCommand =>
        // outputObjs are enough for privilege check, adding inputObjs for consistency with hive
        // behaviour in case of some unexpected issues.
        addDbLevelObjs(d.databaseName, inputObjs)
        addDbLevelObjs(d.databaseName, outputObjs)

      case d: DropFunctionCommand =>
        addFunctionLevelObjs(d.databaseName, d.functionName, outputObjs)

      case d: DropTableCommand => addTableOrViewLevelObjs(d.tableName, outputObjs)

      case i: InsertIntoDataSourceCommand =>
        i.logicalRelation.catalogTable.foreach { table =>
          addTableOrViewLevelObjs(
            table.identifier,
            outputObjs)
        }
        buildQuery(i.query, inputObjs)

      case i if i.nodeName == "InsertIntoDataSourceDirCommand" =>
        buildQuery(getFieldVal(i, "query").asInstanceOf[LogicalPlan], inputObjs)

      case i: InsertIntoHadoopFsRelationCommand =>
        // we are able to get the override mode here, but ctas for hive table with text/orc
        // format and parquet with spark.sql.hive.convertMetastoreParquet=false can success
        // with privilege checking without claiming for UPDATE privilege of target table,
        // which seems to be same with Hive behaviour.
        // So, here we ignore the overwrite mode for such a consistency.
        i.catalogTable foreach { t =>
          addTableOrViewLevelObjs(
            t.identifier,
            outputObjs,
            i.partitionColumns.map(_.name),
            t.schema.fieldNames)
        }
        buildQuery(i.query, inputObjs)

      case i if i.nodeName == "InsertIntoHiveDirCommand" =>
        buildQuery(getFieldVal(i, "query").asInstanceOf[LogicalPlan], inputObjs)

      case i if i.nodeName == "InsertIntoHiveTable" =>
        addTableOrViewLevelObjs(
          getFieldVal(i, "table").asInstanceOf[CatalogTable].identifier, outputObjs)
        buildQuery(getFieldVal(i, "query").asInstanceOf[LogicalPlan], inputObjs)

      case l: LoadDataCommand =>
        addTableOrViewLevelObjs(l.table, outputObjs)
        if (!l.isLocal) {
          inputObjs += new SparkPrivilegeObject(SparkPrivilegeObjectType.DFS_URI, l.path, l.path)
        }

      case s if s.nodeName == "SaveIntoDataSourceCommand" =>
        buildQuery(getFieldVal(s, "query").asInstanceOf[LogicalPlan], outputObjs)

      case s: SetDatabaseCommand => addDbLevelObjs(s.databaseName, inputObjs)

      case s: ShowColumnsCommand => addTableOrViewLevelObjs(s.tableName, inputObjs)

      case s: ShowCreateTableCommand => addTableOrViewLevelObjs(s.table, inputObjs)

      case s: ShowFunctionsCommand => s.db.foreach(addDbLevelObjs(_, inputObjs))

      case s: ShowPartitionsCommand => addTableOrViewLevelObjs(s.tableName, inputObjs)

      case s: ShowTablePropertiesCommand => addTableOrViewLevelObjs(s.table, inputObjs)

      case s: ShowTablesCommand => addDbLevelObjs(s.databaseName, inputObjs)

      case s: TruncateTableCommand => addTableOrViewLevelObjs(s.tableName, outputObjs)

      case _ =>
      // AddFileCommand
      // AddJarCommand
      // AnalyzeColumnCommand
      // ClearCacheCommand
      // CreateTempViewUsing
      // ListFilesCommand
      // ListJarsCommand
      // RefreshTable
      // RefreshTable
      // ResetCommand
      // SetCommand
      // ShowDatabasesCommand
      // StreamingExplainCommand
      // UncacheTableCommand
    }
  }

  /**
   * Add database level spark privilege objects to input or output list
   *
   * @param dbName           database name as spark privilege object
   * @param privilegeObjects input or output list
   */
  private def addDbLevelObjs(
                                dbName: String,
                                privilegeObjects: ArrayBuffer[SparkPrivilegeObject]): Unit = {
    privilegeObjects += new SparkPrivilegeObject(SparkPrivilegeObjectType.DATABASE, dbName, dbName)
  }

  /**
   * Add database level spark privilege objects to input or output list
   *
   * @param dbOption         an option of database name as spark privilege object
   * @param privilegeObjects input or output spark privilege object list
   */
  private def addDbLevelObjs(
                                dbOption: Option[String],
                                privilegeObjects: ArrayBuffer[SparkPrivilegeObject]): Unit = {
    dbOption match {
      case Some(db) =>
        privilegeObjects += new SparkPrivilegeObject(SparkPrivilegeObjectType.DATABASE, db, db)
      case _ =>
    }
  }

  /**
   * Add database level spark privilege objects to input or output list
   *
   * @param identifier       table identifier contains database name as hive privilege object
   * @param privilegeObjects input or output spark privilege object list
   */
  private def addDbLevelObjs(
                                identifier: TableIdentifier,
                                privilegeObjects: ArrayBuffer[SparkPrivilegeObject]): Unit = {
    identifier.database match {
      case Some(db) =>
        privilegeObjects += new SparkPrivilegeObject(SparkPrivilegeObjectType.DATABASE, db, db)
      case _ =>
    }
  }

  /**
   * Add function level spark privilege objects to input or output list
   *
   * @param databaseName     database name
   * @param functionName     function name as spark privilege object
   * @param privilegeObjects input or output list
   */
  private def addFunctionLevelObjs(
                                      databaseName: Option[String],
                                      functionName: String,
                                      privilegeObjects: ArrayBuffer[SparkPrivilegeObject]): Unit = {
    databaseName match {
      case Some(db) =>
        privilegeObjects += new SparkPrivilegeObject(
          SparkPrivilegeObjectType.FUNCTION, db, functionName)
      case _ =>
    }
  }

  /**
   * Add table level spark privilege objects to input or output list
   *
   * @param identifier       table identifier contains database name, and table name as hive
   *                         privilege object
   * @param privilegeObjects input or output list
   * @param mode             Append or overwrite
   */
  private def addTableOrViewLevelObjs(identifier: TableIdentifier,
                                      privilegeObjects: ArrayBuffer[SparkPrivilegeObject], partKeys: Seq[String] = Nil,
                                      columns: Seq[String] = Nil, mode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    LOG.info("*** identifier.database ***" + identifier.database)
    LOG.info("*** identifier.table ***" + identifier.table)

    identifier.database match {
      case Some(db) =>
        val tbName = identifier.table
        val actionType = toActionType(mode)
        privilegeObjects += new SparkPrivilegeObject(
          SparkPrivilegeObjectType.TABLE_OR_VIEW,
          db,
          tbName,
          partKeys,
          columns,
          actionType)
      case _ =>
    }
  }

  /**
   * SparkPrivObjectActionType INSERT or INSERT_OVERWRITE
   *
   * @param mode Append or Overwrite
   */
  private def toActionType(mode: SaveMode): SparkPrivObjectActionType = {
    mode match {
      case SaveMode.Append => SparkPrivObjectActionType.INSERT
      case SaveMode.Overwrite => SparkPrivObjectActionType.INSERT_OVERWRITE
      case _ => SparkPrivObjectActionType.OTHER
    }
  }
}
