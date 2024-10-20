CREATE OR REPLACE FUNCTION rag_search_and_respond(input_params OBJECT)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
AS
$$

/**
 * Validates the input parameters.
 * @param {Object} params - The input parameters to validate.
 * @throws {Error} If any validation check fails.
 */

function validateInput(params) {
    if (typeof params !== 'object' || params === null) {
        throw new Error("Input must be a non-null object");
    }

    if (!params.hasOwnProperty('domain_name')) {
        throw new Error("Missing required parameter: domain_name");
    }

    if (!params.hasOwnProperty('latest_prompts')) {
        throw new Error("Missing required parameter: latest_prompts");
    }

    if (typeof params.domain_name !== 'string' || params.domain_name.trim() === '') {
        throw new Error("domain_name must be a non-empty string");
    }

    if (typeof params.latest_prompts !== 'object' || params.latest_prompts === null) {
        throw new Error("latest_prompts must be a non-null object");
    }

    if (Object.keys(params.latest_prompts).length === 0) {
        throw new Error("latest_prompts must contain at least one prompt");
    }

    for (let key in params.latest_prompts) {
        if (typeof params.latest_prompts[key] !== 'string' || params.latest_prompts[key].trim() === '') {
            throw new Error(`Prompt for key '${key}' must be a non-empty string`);
        }
    }
}

/**
 * Retrieves the appropriate service name for a given domain.
 * @param {string} domain - The domain name to look up.
 * @returns {string} The service name associated with the domain.
 * @throws {Error} If no service is found for the domain and no default service is available.
 */
function get_service_name(domain) {
    const service_query = snowflake.createStatement({
        sqlText: `
        SELECT service_name
        FROM MYGPT.RAG.T_SERVICE_REGISTRY
        WHERE domain_name = :1
        LIMIT 1
        `,
        binds: [domain]
    });
    
    const result = service_query.execute();
    
    if (result.next()) {
        return result.getColumnValue(1);
    } else {
        const default_query = snowflake.createStatement({
            sqlText: `
            SELECT service_name
            FROM MYGPT.RAG.T_SERVICE_REGISTRY
            WHERE domain_name = 'default'
            LIMIT 1
            `
        });
