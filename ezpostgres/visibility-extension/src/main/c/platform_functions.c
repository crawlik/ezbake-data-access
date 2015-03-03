/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

#include "postgres.h"
#include "utils/array.h"
#include "utils/guc.h"

#include "ezbake_purge.h"
#include "ezbake_serialization.h"
#include "ezbake_permissions.h"

#define EZBAKE_TOKEN_SETTING "ezbake.token"

char *dup_pg_varchar(const VarChar * const pg_varchar) {
    size_t len = VARSIZE(pg_varchar) - VARHDRSZ;
    char *dup = malloc(len + 1); /* Must add NULL-terminator */
    memcpy(dup, (char *)VARDATA(pg_varchar), len);
    dup[len] = '\0'; /* Add NULL-terminator */

    return dup;
}

visibility_handle_t *deserialize_vis(const VarChar * const base64_varchar) {
    char *error = NULL;
    char *base64 = dup_pg_varchar(base64_varchar);

    visibility_handle_t *vis =
        ezbake_deserialize_visibility_base64(base64, &error);

    if (error) {
        ereport(ERROR, (errmsg("Error deserializing visibility: %s", error)));
        free(error);
        vis = NULL;
    }

    free(base64);

    return vis;
}

authorizations_handle_t *deserialize_auths(
        const VarChar * const base64_varchar) {
    char *error = NULL;
    char *base64 = dup_pg_varchar(base64_varchar);

    authorizations_handle_t *auths =
        ezbake_deserialize_authorizations_base64(base64, &error);

    if (error) {
        ereport(ERROR,
                (errmsg("Error deserializing authorizations: %s", error)));

        free(error);
        auths = NULL;
    }

    free(base64);

    return auths;
}

/*
 * Returns true if the table row is visible given the user's auths.
 */
PG_FUNCTION_INFO_V1(verify_row_visible);
Datum verify_row_visible(PG_FUNCTION_ARGS) {
    VarChar *auths_base64 = PG_GETARG_VARCHAR_P(0);
    VarChar *vis_base64 = PG_GETARG_VARCHAR_P(1);

    authorizations_handle_t *auths = deserialize_auths(auths_base64);
    if (!auths) {
        ereport(ERROR,
                (errmsg("There was an error deserializing the "
                        "authorizations!")));

        PG_RETURN_BOOL(false);
    }

    visibility_handle_t *vis = deserialize_vis(vis_base64);
    if (!vis) {
        ereport(ERROR,
                (errmsg("There was an error deserializing the visibility!")));

        ezbake_authorizations_handle_free(auths);
        PG_RETURN_BOOL(false);
    }

    char *error = NULL;
    uint32_t permissions = ezbake_get_user_permissions(auths, vis, &error);
    if (error) {
        ereport(ERROR, (errmsg("Error evaluating permissions: %s", error)));
        ezbake_authorizations_handle_free(auths);
        ezbake_visibility_handle_free(vis);
        free(error);
        PG_RETURN_BOOL(false);
    }

    ezbake_authorizations_handle_free(auths);
    ezbake_visibility_handle_free(vis);
    bool is_authorized =
        (permissions & EZBAKE_USER_PERM_READ) &&
        (permissions & EZBAKE_USER_PERM_WRITE) &&
        (permissions & EZBAKE_USER_PERM_MANAGE_VISIBILITY);

    PG_RETURN_BOOL(is_authorized);
}

/*
 * Returns true if the table row is visible given the user's auths.
 */
PG_FUNCTION_INFO_V1(verify_row_visible_current_setting);
Datum verify_row_visible_current_setting(PG_FUNCTION_ARGS) {
    const VarChar *vis_base64 = PG_GETARG_VARCHAR_P(0);

    const char *token_base64 =
        GetConfigOption(EZBAKE_TOKEN_SETTING, false, false);

    if (!token_base64) {
        ereport(ERROR,
                (errmsg("Could not read serialized security token from session "
                        "config with key " EZBAKE_TOKEN_SETTING)));

        PG_RETURN_BOOL(false);
    }

    char *error = NULL;
    token_handle_t *token =
        ezbake_deserialize_token_base64(token_base64, &error);

    if (error) {
        ereport(ERROR,
                (errmsg("Error deserializing the security token: %s", error)));

        free(error);
        PG_RETURN_BOOL(false);
    }

    authorizations_handle_t *auths =
        ezbake_get_authorizations_from_token(token, &error);

    if (error) {
        ereport(ERROR,
                (errmsg("Error extracting auths from security token: %s",
                        error)));

        ezbake_token_handle_free(token);
        free(error);
        PG_RETURN_BOOL(false);
    }

    visibility_handle_t *vis = deserialize_vis(vis_base64);
    if (!vis) {
        ereport(ERROR,
                (errmsg("There was an error deserializing the visibility!")));

        ezbake_token_handle_free(token);
        ezbake_authorizations_handle_free(auths);
        PG_RETURN_BOOL(false);
    }

    uint32_t permissions = ezbake_get_user_permissions(auths, vis, &error);
    if (error) {
        ereport(ERROR, (errmsg("Error evaluating permissions: %s", error)));
        ezbake_token_handle_free(token);
        ezbake_authorizations_handle_free(auths);
        ezbake_visibility_handle_free(vis);
        free(error);
        PG_RETURN_BOOL(false);
    }

    ezbake_token_handle_free(token);
    ezbake_authorizations_handle_free(auths);
    ezbake_visibility_handle_free(vis);

    bool is_authorized =
        (permissions & EZBAKE_USER_PERM_READ) &&
        (permissions & EZBAKE_USER_PERM_WRITE) &&
        (permissions & EZBAKE_USER_PERM_MANAGE_VISIBILITY);

    PG_RETURN_BOOL(is_authorized);
}

/*
 * Returns true if the given visibility has composite set to true.
 */
PG_FUNCTION_INFO_V1(is_composite);
Datum is_composite(PG_FUNCTION_ARGS) {
    VarChar *vis_base64 = PG_GETARG_VARCHAR_P(0);
    visibility_handle_t *vis = deserialize_vis(vis_base64);
    if (!vis) {
        PG_RETURN_BOOL(false);
    }

    char *error = NULL;
    purge_info_t purge_info;
    purge_info.composite = false;
    ezbake_get_purge_info(vis, &purge_info, &error);
    if (error) {
        ereport(ERROR, (errmsg("Error getting the purge info from visibility! %s", error)));
        free(error);
    }

    ezbake_visibility_handle_free(vis);
    PG_RETURN_BOOL(purge_info.composite);
}

/*
 * Returns the purge ID as a bigint. Returns -1 on error.
 */
PG_FUNCTION_INFO_V1(get_purge_id);
Datum get_purge_id(PG_FUNCTION_ARGS) {
    VarChar *vis_base64 = PG_GETARG_VARCHAR_P(0);
    visibility_handle_t *vis = deserialize_vis(vis_base64);
    if (!vis) {
        PG_RETURN_BOOL(false);
    }

    char *error = NULL;
    purge_info_t purge_info;
    purge_info.id = -1;
    ezbake_get_purge_info(vis, &purge_info, &error);
    if (error) {
        ereport(ERROR, (errmsg("Error getting the purge info from visibility! %s", error)));
        free(error);
    }

    ezbake_visibility_handle_free(vis);
    PG_RETURN_INT64(purge_info.id);
}

/*
 * Returns true if a row should be purged based on a purge vector
 */
#pragma GCC diagnostic ignored "-Wsign-conversion"
PG_FUNCTION_INFO_V1(should_purge);
Datum should_purge(PG_FUNCTION_ARGS) {
    ArrayType *purge_vector = PG_GETARG_ARRAYTYPE_P(0);
    VarChar *vis_base64 = PG_GETARG_VARCHAR_P(1);

    int64 *purge_vector_data = (int64 *) ARR_DATA_PTR(purge_vector);
    visibility_handle_t *vis = deserialize_vis(vis_base64);
    if (!vis) {
        PG_RETURN_BOOL(false);
    }

    char *error = NULL;
    bool result = ezbake_should_purge(
            (int64_t *) purge_vector_data,
            (size_t) ARR_DIMS(purge_vector)[0], vis, &error);

    if (error) {
        ereport(ERROR, (errmsg("Error testing purge vectors: %s", error)));
        free(error);
        result = false;
    }

    ezbake_visibility_handle_free(vis);
    PG_RETURN_BOOL(result);
}
