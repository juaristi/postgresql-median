#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <utils/tuplesort.h>
#include <utils/typcache.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

struct median {
	uint32 num_elems;
	Tuplesortstate *tss;
};

static void initialize_state(MemoryContext agg_context, bytea **state)
{
	struct median *ms;
	TypeCacheEntry *typc;
	Size len = VARHDRSZ + sizeof(struct median);

	*state = MemoryContextAllocZero(agg_context, len);
	if (!*state)
		elog(ERROR, "could not initialize state");

	SET_VARSIZE(*state, len);

	/* Initialize tuplesort state */
	typc = lookup_type_cache(INT4OID, TYPECACHE_LT_OPR);
	if (!typc)
		elog(ERROR, "could not get type cache entry for INT4OID");

	ms = (struct median *) VARDATA(*state);
	ms->tss = tuplesort_begin_datum(
			INT4OID,	/* datum type */
			typc->lt_opr,	/* less-than operator */
			InvalidOid,	/* no collation */
			0,
			5000,
			0);		/* no random access */
	if (!ms->tss)
		elog(ERROR, "could not initialize tuplesort state");
}

PG_FUNCTION_INFO_V1(median_transfn);

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
	struct median *ms;
	MemoryContext agg_context;
	bytea *state = (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_transfn called in non-aggregate context");
	if (PG_NARGS() < 2)
		elog(ERROR, "too few arguments");

	if (!state)
		initialize_state(agg_context, &state);

	if (!PG_ARGISNULL(1))	/* skip null values */
	{
		ms = (struct median *) VARDATA(state);

		tuplesort_putdatum(ms->tss, PG_GETARG_INT32(1), 0);
		ms->num_elems++;
	}

	PG_RETURN_BYTEA_P(state);
}

PG_FUNCTION_INFO_V1(median_finalfn);

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
Datum
median_finalfn(PG_FUNCTION_ARGS)
{
	Datum val;
	bool is_null;
	uint32 pos;
	struct median *ms;
	MemoryContext agg_context;
#ifdef PRINT_TUPLESORT_STATS
	long space_used = 0;
	const char *sort_method = NULL, *space_type = NULL;
#endif

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");
	if (PG_NARGS() < 2)
		elog(ERROR, "too few arguments");

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	ms = (struct median *) VARDATA(PG_GETARG_BYTEA_P(0));

	tuplesort_performsort(ms->tss);

#ifdef PRINT_TUPLESORT_STATS
	tuplesort_get_stats(ms->tss, &sort_method, &space_type, &space_used);

	elog(NOTICE, "Sort method: %s", sort_method);
	elog(NOTICE, "Space type: %s", space_type);
	elog(NOTICE, "Space used: %ld KB", space_used);
#endif

	pos = ms->num_elems / 2;

	if (!tuplesort_skiptuples(ms->tss, pos, 1))
		elog(ERROR, "could not advance %d slots", pos);
	if (!tuplesort_getdatum(ms->tss, 1, &val, &is_null, NULL))
		elog(ERROR, "could not get element after advancing %d slots", pos);
	if (is_null)
		elog(ERROR, "element at position %d is null - this should not happen", pos);

	PG_RETURN_INT32(val);
}
