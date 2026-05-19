
#include "postgres.h"

#include "c.h"

#include "optimizer/planner.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "catalog/pg_class_d.h"
#include "catalog/pg_class.h"

#include "catalog/pg_namespace_d.h"
#include "catalog/pg_namespace.h"
#if PG_VERSION_NUM < 140000
#include "catalog/indexing.h"
#endif

#include "utils/fmgroids.h"
#include "utils/snapmgr.h"

#include "access/table.h"
#include "access/tableam.h"
#include "access/genam.h"

#include "access/genam.h"

#include "fmgr.h"

#include "tcop/utility.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(spqrguard_share_key_range);

#if PG_VERSION_NUM >= 180000
static void spqrguard_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count);
#else
static void spqrguard_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once);
#endif
static void spqrguard_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
					bool readOnlyTree,
					ProcessUtilityContext context,
					ParamListInfo params, QueryEnvironment *queryEnv,
					DestReceiver *dest, QueryCompletion *qc);


static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

const struct config_enum_entry spqrguard_guc_opts[] = {
    {"unset", 0, false},
    {"on", 1, false},
    {"off", 2, false},
    {NULL, 0, false}
};

static int prevent_distributed_table_modify = 0;
static int prevent_reference_table_modify = 0;
static bool key_range_lock = false;
static bool any_modification = false;

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "This module can only be loaded via shared_preload_libraries");
		return;
	}

    DefineCustomEnumVariable("spqrguard.prevent_reference_table_modify",
                            "Restrict sql referencing one of SPQR reference relations to be read-only",
                            "Defaults to 'unset'",
                            &prevent_reference_table_modify,
                            0,
                            spqrguard_guc_opts,
                            PGC_USERSET,
                            GUC_NOT_IN_SAMPLE,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomEnumVariable("spqrguard.prevent_distributed_table_modify",
                            "Restrict sql referencing one of SPQR distributed relations to be read-only",
                            "Defaults to unset",
                            &prevent_distributed_table_modify,
                            0,
                            spqrguard_guc_opts,
                            PGC_USERSET,
                            GUC_NOT_IN_SAMPLE,
                            NULL,
                            NULL,
                            NULL);

#if PG_VERSION_NUM >= 130000
	prev_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = spqrguard_ExecutorRun;
    prev_ProcessUtility_hook = ProcessUtility_hook;
    ProcessUtility_hook = spqrguard_ProcessUtility;
#endif

}

typedef struct spqrguard_distributedRelations {
    Oid spqr_d_metadata_reloid;
    Oid spqr_ref_metadata_reloid;
    Oid spqr_trans_ref_metadata_reloid;
    Oid spqr_local_key_ranges_reloid;
    Oid spqr_local_key_ranges_pkey_reloid;

    Oid spqr_global_settings_reloid;

    bool initialized;

    bool prevent_distributed_table_modify;
    bool prevent_reference_table_modify;
    bool prevent_transferred_reference_table_modify;
} spqrguard_distributedRelations; 


void
spqrguard_ExecutorRun(QueryDesc *queryDesc,
					 ScanDirection direction, uint64 count
#if PG_VERSION_NUM < 180000
                     , bool execute_once
#endif
                    );


static bool spqrguard_check_relation(spqrguard_distributedRelations *cxt, Oid relid) {
    /* NOOP for now */
    Relation spqrrel;
    SysScanDesc scan;
    HeapTuple tuple;
    bool    res;
    ScanKeyData skey[1];

    res = false;

    /* SELECT FROM pg_catalog.pg_namespace WHERE nspname = 'spqr_metadata */
    /**/
    if (!cxt->initialized)
    {
	    res = true;
	    return res;
    }
    spqrrel = try_table_open(cxt->spqr_d_metadata_reloid, AccessShareLock);
    if (spqrrel == NULL) {
        elog(WARNING, "table spqr_metadata.spqr_distributed_relations does not exist");
        return false;
    }

#define Anum_spqr_distributed_relations_reloid 1

    ScanKeyInit(&skey[0], Anum_spqr_distributed_relations_reloid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));

    scan = systable_beginscan(spqrrel, InvalidOid, false, NULL, 1, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
        res = true;
    }

    table_close(spqrrel, AccessShareLock);
    systable_endscan(scan);

    return res;
}

static bool spqrguard_check_transferred_ref_relation(spqrguard_distributedRelations *cxt, Oid relid) {
    /* NOOP for now */
    Relation spqrrel;
    SysScanDesc scan;
    HeapTuple tuple;
    bool    res;
    ScanKeyData skey[1];

    res = false;

    /* SELECT FROM pg_catalog.pg_namespace WHERE nspname = 'spqr_metadata */
    /**/
    if (!cxt->initialized)
    {
	    res = true;
	    return res;
    }
    spqrrel = try_table_open(cxt->spqr_trans_ref_metadata_reloid, AccessShareLock);
    if (spqrrel == NULL) {
        elog(WARNING, "table spqr_metadata.spqr_transferred_reference_relations does not exist");
        return false;
    }

#define Anum_spqr_transferred_reference_relations_reloid 1

    ScanKeyInit(&skey[0], Anum_spqr_transferred_reference_relations_reloid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));

    scan = systable_beginscan(spqrrel, InvalidOid, false, NULL, 1, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
        res = true;
    }

    table_close(spqrrel, AccessShareLock);
    systable_endscan(scan);

    return res;
}

static bool spqrguard_check_ref_relation(spqrguard_distributedRelations *cxt, Oid relid) {
    /* NOOP for now */
    Relation spqrrel;
    SysScanDesc scan;
    HeapTuple tuple;
    bool    res;
    ScanKeyData skey[1];

    res = false;

    /* SELECT FROM pg_catalog.pg_namespace WHERE nspname = 'spqr_metadata */
    /**/
    if (!cxt->initialized)
    {
	    res = true;
	    return res;
    }
    spqrrel = try_table_open(cxt->spqr_ref_metadata_reloid, AccessShareLock);
    if (spqrrel == NULL) {
        elog(WARNING, "table spqr_metadata.spqr_reference_relations does not exist");
        return false;
    }

#define Anum_spqr_reference_relations_reloid 1

    ScanKeyInit(&skey[0], Anum_spqr_reference_relations_reloid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));

    scan = systable_beginscan(spqrrel, InvalidOid, false, NULL, 1, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
        res = true;
    }

    table_close(spqrrel, AccessShareLock);
    systable_endscan(scan);

    return res;
}

/*

typedef bool (*planstate_tree_walker_callback) (struct PlanState *planstate,
												void *context);
*/

static bool spqrguard_planstate_walker(struct PlanState *planstate,
												void *context) {
    if (IsA(planstate, ModifyTableState)) {
        ModifyTableState *mts;
        Oid relid;

        spqrguard_distributedRelations *drs = context;

        mts = (ModifyTableState*) planstate;

        relid = RelationGetRelid(mts->resultRelInfo->ri_RelationDesc);

        if (spqrguard_check_relation(drs, relid)) {
            if (drs->prevent_distributed_table_modify)
                elog(ERROR, "unable to modify SPQR distributed relation within read-only transaction");
        }
        
        if (spqrguard_check_ref_relation(drs, relid)) {
            any_modification = true;
            if (drs->prevent_reference_table_modify)
                elog(ERROR, "unable to modify SPQR reference relation within read-only transaction");
        }

        if (spqrguard_check_transferred_ref_relation(drs, relid)) {
            any_modification = true;
            if (drs->prevent_transferred_reference_table_modify)
                elog(ERROR, "unable to modify SPQR reference relation within read-only transaction");
        }
    }

    return false;
}


static const char * spqrguard_dr_relname = "spqr_distributed_relations";
static const char * spqrguard_ref_relname = "spqr_reference_relations";
static const char * spqrguard_transferred_ref_relname = "spqr_transferred_reference_relations";
static const char * spqrguard_dr_schema = "spqr_metadata";
static const char * spqrguard_global_settings = "spqr_global_settings";
static const char * spqrguard_local_key_ranges = "spqr_local_key_ranges";
static const char * spqrguard_local_key_ranges_pkey = "spqr_local_key_ranges_pkey";

/* It would be more handy to have FIXED-oid relations... */


static Oid SPQRGResolveMetadataSchemaOid() {
    Relation nsprel;
    SysScanDesc scan;
    HeapTuple tuple;
    Oid MetadataSchemaOid;
    ScanKeyData skey[1];
    Form_pg_namespace nsp_type;

    MetadataSchemaOid = InvalidOid;

    /* SELECT FROM pg_catalog.pg_namespace WHERE nspname = 'spqr_metadata */
    /**/
    
    nsprel = table_open(NamespaceRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_namespace_nspname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(spqrguard_dr_schema));

    scan = systable_beginscan(nsprel, NamespaceNameIndexId, true, NULL, 1, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
	    nsp_type = (Form_pg_namespace) GETSTRUCT(tuple);
        MetadataSchemaOid = nsp_type->oid;
    }

    table_close(nsprel, RowExclusiveLock);
    systable_endscan(scan);

    return MetadataSchemaOid;
}

static Oid SPQRGResolveDistrRelOid(Oid MetadataSchemaOid) {
    Relation classrel;
    SysScanDesc scan;
    HeapTuple tuple;
    Oid DistrRelOid;
    ScanKeyData skey[2];
    Form_pg_class class_type;

    DistrRelOid = InvalidOid;
    
    /* SELECT FROM pg_catalog.pg_class WHERE relname = 'spqr_distributed_relations '
    * and relnamespace = $oid; */
    /**/
    
    classrel = table_open(RelationRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(spqrguard_dr_relname));

    ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(MetadataSchemaOid));

    scan = systable_beginscan(classrel, ClassNameNspIndexId, true, NULL, 2, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
	    class_type = (Form_pg_class) GETSTRUCT(tuple);
        DistrRelOid = class_type->oid;
    }

    table_close(classrel, RowExclusiveLock);
    systable_endscan(scan);

    return DistrRelOid;
}

static Oid SPQRGResolveTransferredReferenceRelOid(Oid MetadataSchemaOid) {
    Relation classrel;
    SysScanDesc scan;
    HeapTuple tuple;
    Oid TransferredReferenceRelOid;
    ScanKeyData skey[2];
    Form_pg_class class_type;

    TransferredReferenceRelOid = InvalidOid;
    
    /* SELECT FROM pg_catalog.pg_class WHERE relname = 'spqr_distributed_relations '
    * and relnamespace = $oid; */
    /**/
    
    classrel = table_open(RelationRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(spqrguard_transferred_ref_relname));

    ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(MetadataSchemaOid));

    scan = systable_beginscan(classrel, ClassNameNspIndexId, true, NULL, 2, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
	    class_type = (Form_pg_class) GETSTRUCT(tuple);
        TransferredReferenceRelOid = class_type->oid;
    }

    table_close(classrel, RowExclusiveLock);
    systable_endscan(scan);

    return TransferredReferenceRelOid;
}

static Oid SPQRGResolveReferenceRelOid(Oid MetadataSchemaOid) {
    Relation classrel;
    SysScanDesc scan;
    HeapTuple tuple;
    Oid ReferenceRelOid;
    ScanKeyData skey[2];
    Form_pg_class class_type;

    ReferenceRelOid = InvalidOid;
    
    /* SELECT FROM pg_catalog.pg_class WHERE relname = 'spqr_distributed_relations '
    * and relnamespace = $oid; */
    /**/
    
    classrel = table_open(RelationRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(spqrguard_ref_relname));

    ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(MetadataSchemaOid));

    scan = systable_beginscan(classrel, ClassNameNspIndexId, true, NULL, 2, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
	    class_type = (Form_pg_class) GETSTRUCT(tuple);
        ReferenceRelOid = class_type->oid;
    }

    table_close(classrel, RowExclusiveLock);
    systable_endscan(scan);

    return ReferenceRelOid;
}

static Oid SPQRGResolveGlobalSettingsOid(Oid MetadataSchemaOid) {
    Relation classrel;
    SysScanDesc scan;
    HeapTuple tuple;
    Oid SetRelOid;
    ScanKeyData skey[2];
    Form_pg_class class_type;

    SetRelOid = InvalidOid;
    
    /* SELECT FROM pg_catalog.pg_class WHERE relname = 'spqr_distributed_relations '
    * and relnamespace = $oid; */
    /**/
    
    classrel = table_open(RelationRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(spqrguard_global_settings));

    ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(MetadataSchemaOid));

    scan = systable_beginscan(classrel, ClassNameNspIndexId, true, NULL, 2, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
	    class_type = (Form_pg_class) GETSTRUCT(tuple);
        SetRelOid = class_type->oid;
    }

    table_close(classrel, RowExclusiveLock);
    systable_endscan(scan);

    return SetRelOid;
}

static Oid SPQRGResolveLocalKeyRangesIndexOid(Oid MetadataSchemaOid) {
    Relation classrel;
    SysScanDesc scan;
    HeapTuple tuple;
    Oid LocalKeyRangesIndOid;
    ScanKeyData skey[2];
    Form_pg_class class_type;

    LocalKeyRangesIndOid = InvalidOid;
    
    /* SELECT FROM pg_catalog.pg_class WHERE relname = 'spqr_distributed_relations '
    * and relnamespace = $oid; */
    /**/
    
    classrel = table_open(RelationRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(spqrguard_local_key_ranges_pkey));

    ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(MetadataSchemaOid));

    scan = systable_beginscan(classrel, ClassNameNspIndexId, true, NULL, 2, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
	    class_type = (Form_pg_class) GETSTRUCT(tuple);
        LocalKeyRangesIndOid = class_type->oid;
    }

    table_close(classrel, RowExclusiveLock);
    systable_endscan(scan);

    return LocalKeyRangesIndOid;
}

static Oid SPQRGResolveLocalKeyRangesOid(Oid MetadataSchemaOid) {
    Relation classrel;
    SysScanDesc scan;
    HeapTuple tuple;
    Oid LocalKeyRangesOid;
    ScanKeyData skey[2];
    Form_pg_class class_type;

    LocalKeyRangesOid = InvalidOid;
    
    /* SELECT FROM pg_catalog.pg_class WHERE relname = 'spqr_distributed_relations '
    * and relnamespace = $oid; */
    /**/
    
    classrel = table_open(RelationRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(spqrguard_local_key_ranges));

    ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(MetadataSchemaOid));

    scan = systable_beginscan(classrel, ClassNameNspIndexId, true, NULL, 2, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
	    class_type = (Form_pg_class) GETSTRUCT(tuple);
        LocalKeyRangesOid = class_type->oid;
    }

    table_close(classrel, RowExclusiveLock);
    systable_endscan(scan);

    return LocalKeyRangesOid;
}

typedef struct Form_DataGlobalSettings {
    int32_t name;
    bool value;
} Form_DataGlobalSettings;

typedef Form_DataGlobalSettings *Form_GlobalSettings;

#define Anum_spqr_global_settings_name 1
#define Anum_spqr_global_settings_value 2

#define PREVENT_DISTRIBUTED_TABLE_MODIFY 42
#define PREVENT_REFERENCE_TABLE_MODIFY 69
#define PREVENT_TRANSFERRED_REFERENCE_TABLE_MODIFY 70
#define PREVENT_KEY_RANGE_MODIFY_ON_LOCK 66

static bool ResolveGlobalBoolSetting(Oid setReloid, int32_t setname) {
    Relation setrel;
#define ResolveGlobalBoolSetCols 1
    ScanKeyData skey[ResolveGlobalBoolSetCols];
    TableScanDesc desc;
    TupleTableSlot *slot;
    bool val;
    
    setrel = try_table_open(setReloid, AccessShareLock);
    if (setrel == NULL) {
        elog(WARNING, "table spqr_metadata.spqr_global_settings does not exist");
        return false;
    }
    /* default */
    val = false;
    
    ScanKeyInit(&skey[0], Anum_spqr_global_settings_name,
              BTEqualStrategyNumber, F_INT4EQ,
              Int32GetDatum(setname));
              
    desc = table_beginscan(setrel, SnapshotSelf, ResolveGlobalBoolSetCols, skey);
    
    slot = table_slot_create(setrel, NULL);

    if (table_scan_getnextslot(desc, ForwardScanDirection, slot)) {

        HeapTuple tuple;
        tuple = ExecFetchSlotHeapTuple(slot, false, NULL);

        val = ((Form_GlobalSettings) GETSTRUCT(tuple))->value;
    }

    ExecDropSingleTupleTableSlot(slot);

    table_endscan(desc);
    table_close(setrel, AccessShareLock);

    return val;
}

static void populate_spqrguard(spqrguard_distributedRelations *cxt) {
    if (cxt->spqr_d_metadata_reloid == InvalidOid)
    {
        Oid spqrguard_dr_schema_oid = SPQRGResolveMetadataSchemaOid();
        if (spqrguard_dr_schema_oid == InvalidOid) {
            /* extension not created yet, but hook is already in-place. */
            cxt->initialized = false;
        } else {
            cxt->initialized = true;
            cxt->spqr_d_metadata_reloid = SPQRGResolveDistrRelOid(spqrguard_dr_schema_oid);
            cxt->spqr_ref_metadata_reloid = SPQRGResolveReferenceRelOid(spqrguard_dr_schema_oid);
            cxt->spqr_trans_ref_metadata_reloid = SPQRGResolveTransferredReferenceRelOid(spqrguard_dr_schema_oid);
            cxt->spqr_global_settings_reloid = SPQRGResolveGlobalSettingsOid(spqrguard_dr_schema_oid);
            cxt->spqr_local_key_ranges_reloid = SPQRGResolveLocalKeyRangesOid(spqrguard_dr_schema_oid);
            cxt->spqr_local_key_ranges_pkey_reloid = SPQRGResolveLocalKeyRangesIndexOid(spqrguard_dr_schema_oid);
        }
    }

    if (cxt->spqr_global_settings_reloid == InvalidOid) {
        cxt->prevent_distributed_table_modify = false;
        cxt->prevent_reference_table_modify = false;
        cxt->prevent_transferred_reference_table_modify = false;
    } else {
        cxt->prevent_distributed_table_modify = 
            ResolveGlobalBoolSetting(cxt->spqr_global_settings_reloid, PREVENT_DISTRIBUTED_TABLE_MODIFY);
        cxt->prevent_reference_table_modify = 
            ResolveGlobalBoolSetting(cxt->spqr_global_settings_reloid, PREVENT_REFERENCE_TABLE_MODIFY);
        cxt->prevent_transferred_reference_table_modify = 
            ResolveGlobalBoolSetting(cxt->spqr_global_settings_reloid, PREVENT_TRANSFERRED_REFERENCE_TABLE_MODIFY);
    }

    /* Session-level GUC is allowed to override default to true, not vise-versa */
    switch (prevent_distributed_table_modify) 
    {
    case 1:
        cxt->prevent_distributed_table_modify = true;
        break;
    case 2:
        cxt->prevent_distributed_table_modify = false;
        break;
    default:
        break;
    }
    switch (prevent_reference_table_modify)
    {
    case 1:
        cxt->prevent_reference_table_modify = true;
        break;
    case 2:
        cxt->prevent_reference_table_modify = false;
        break;
    default:
        break;
    }
    if (key_range_lock && ResolveGlobalBoolSetting(cxt->spqr_global_settings_reloid, PREVENT_KEY_RANGE_MODIFY_ON_LOCK)) {
        cxt -> prevent_distributed_table_modify = true;
    }
}

static spqrguard_distributedRelations cxt;

#define Anum_spqr_local_key_range_key_range_id 1
// XXX: do we need release?
Datum spqrguard_share_key_range (PG_FUNCTION_ARGS) {
    Relation kr_rel;
    Relation kr_ind;
    #define ResolveKeyRangeMetaCols 1
    ScanKeyData skey[ResolveKeyRangeMetaCols];
    IndexScanDesc desc;
    TupleTableSlot *slot = NULL;
    bool val;

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errmsg("key_range_id cannot be NULL")));
    }

    populate_spqrguard(&cxt);
    
    // check if key range is in local_key_ranges table
    if (!cxt.initialized || cxt.spqr_global_settings_reloid == InvalidOid ) {
        /* TODO: mb panic? should not happen */
        PG_RETURN_BOOL(false);
    }
    if (!ResolveGlobalBoolSetting(cxt.spqr_global_settings_reloid, PREVENT_KEY_RANGE_MODIFY_ON_LOCK)) {
        PG_RETURN_BOOL(false);
    }

    kr_rel = try_table_open(cxt.spqr_local_key_ranges_reloid, AccessShareLock);
    if (kr_rel == NULL) {
        elog(WARNING, "table spqr_metadata.spqr_local_key_ranges does not exist");
        PG_RETURN_BOOL(false);
    }

    kr_ind = try_index_open(cxt.spqr_local_key_ranges_pkey_reloid, AccessShareLock);
    if (kr_ind == NULL) {
        elog(WARNING, "index spqr_metadata.spqr_local_key_ranges_pkey does not exist");
        PG_RETURN_BOOL(false);
    }
    /* default */
    val = false;
    
    ScanKeyInit(&skey[0], Anum_spqr_local_key_range_key_range_id,
            BTEqualStrategyNumber, F_TEXTEQ,
            PG_GETARG_DATUM(0));

    slot = table_slot_create(kr_rel, NULL);

#if PG_VERSION_NUM >= 180000
    desc = index_beginscan(kr_rel, kr_ind,
									 SnapshotSelf, NULL, ResolveKeyRangeMetaCols, 0);
#else
    desc = index_beginscan(kr_rel, kr_ind,
									 SnapshotSelf, ResolveKeyRangeMetaCols, 0);
#endif

    index_rescan(desc, skey, ResolveKeyRangeMetaCols, NULL, 0);

    if (index_getnext_slot(desc, ForwardScanDirection, slot)) {
        val = true;
    }

    if (val) {
        // lock distributed relations for update
        key_range_lock = true;
    }

    ExecDropSingleTupleTableSlot(slot);
    index_endscan(desc);
    table_close(kr_rel, NoLock);
    table_close(kr_ind, AccessShareLock);

    PG_RETURN_BOOL(val);
}


#if PG_VERSION_NUM >= 180000
static void
spqrguard_ExecutorRun(QueryDesc *queryDesc,
					 ScanDirection direction, uint64 count)
{
    populate_spqrguard(&cxt);

    spqrguard_planstate_walker(queryDesc->planstate, &cxt);

    (void)planstate_tree_walker(queryDesc->planstate, spqrguard_planstate_walker,
								 &cxt);

    if (prev_ExecutorRun_hook)
        (void) prev_ExecutorRun_hook(queryDesc, direction, count);
    else
        (void) standard_ExecutorRun(queryDesc, direction, count);
}
#else
void
spqrguard_ExecutorRun(QueryDesc *queryDesc,
					 ScanDirection direction, uint64 count, bool execute_once)
{
    populate_spqrguard(&cxt);

    spqrguard_planstate_walker(queryDesc->planstate, &cxt);

    (void)planstate_tree_walker(queryDesc->planstate, spqrguard_planstate_walker,
								 &cxt);

    if (prev_ExecutorRun_hook)
        (void) prev_ExecutorRun_hook(queryDesc, direction, count, execute_once);
    else
        (void) standard_ExecutorRun(queryDesc, direction, count, execute_once);
}
#endif

// TODO: executor run => utility shit && lock settings table
static void
spqrguard_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
					bool readOnlyTree,
					ProcessUtilityContext context,
					ParamListInfo params, QueryEnvironment *queryEnv,
					DestReceiver *dest, QueryCompletion *qc)
{
	Node	   *parsetree = pstmt->utilityStmt;
    if (IsA(parsetree, TransactionStmt)) {
        TransactionStmt *stmt = (TransactionStmt *) parsetree;
        if (stmt->kind == TRANS_STMT_COMMIT || stmt->kind == TRANS_STMT_PREPARE) { // mb prepare transaction too
            populate_spqrguard(&cxt);

            if (any_modification) {                
			    LockRelationOid(cxt.spqr_global_settings_reloid, AccessShareLock);
                populate_spqrguard(&cxt);
                if (cxt.prevent_distributed_table_modify) {
                    elog(ERROR, "unable to modify SPQR distributed relation within read-only transaction");
                }
            }

        }
        if (stmt->kind == TRANS_STMT_COMMIT || stmt->kind == TRANS_STMT_PREPARE || stmt->kind == TRANS_STMT_ROLLBACK || stmt->kind == TRANS_STMT_BEGIN) {
            any_modification = false;
        }
    }  



    {
        if (prev_ProcessUtility_hook)
            prev_ProcessUtility_hook(pstmt, queryString, readOnlyTree,
                                context, params, queryEnv,
                                dest, qc);
        else
            standard_ProcessUtility(pstmt, queryString, readOnlyTree,
                                    context, params, queryEnv,
                                    dest, qc);
    }
}