CREATE OR REPLACE PROCEDURE rag_search_and_respond(INPUT_PARAMS OBJECT)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
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

    // Validate service_id
    if (!params.hasOwnProperty('service_id')) {
        throw new Error("Missing required parameter: service_id");
    }
    // Check if service_id is a number or can be converted to one
    const serviceId = Number(params.service_id);
    if (isNaN(serviceId) || !Number.isInteger(serviceId) || serviceId <= 0) {
        throw new Error("service_id must be a positive integer");
    }

    // Validate latest_prompts
    if (!params.hasOwnProperty('latest_prompts')) {
        throw new Error("Missing required parameter: latest_prompts");
    }
    if (typeof params.latest_prompts !== 'object' || params.latest_prompts === null) {
        throw new Error("latest_prompts must be a non-null object");
    }
    if (Object.keys(params.latest_prompts).length === 0) {
        throw new Error("latest_prompts must contain at least one prompt");
    }
    if (Object.keys(params.latest_prompts).length > 20) {
        throw new Error("latest_prompts cannot contain more than 20 prompts");
    }

    // Validate each prompt
    for (let key in params.latest_prompts) {
        // Validate epoch timestamp key
        if (isNaN(Number(key)) || Number(key) <= 0) {
            throw new Error(`Invalid epoch timestamp key: ${key}`);
        }
        // Validate prompt value
        if (typeof params.latest_prompts[key] !== 'string' || params.latest_prompts[key].trim() === '') {
            throw new Error(`Prompt for epoch ${key} must be a non-empty string`);
        }
    }

    // Validate debug flag (optional)
    if (params.hasOwnProperty('debug') && typeof params.debug !== 'boolean') {
        throw new Error("debug flag must be a boolean value");
    }
}

/**
 * Logs a message if debug mode is enabled
 * @param {string} message - The message to log
 * @param {boolean} debug - Whether debug mode is enabled
 */
function logDebug(message, debug) {
    if (debug) {
        snowflake.createStatement({
            sqlText: "CALL system$log_info(?)",
            binds: [message]
        }).execute();
    }
}

/**
 * Main procedure execution
 */
try {
    // Input validation
    validateInput(INPUT_PARAMS);

    const service_id = INPUT_PARAMS.service_id;
    const latest_prompts = INPUT_PARAMS.latest_prompts;
    const debug = INPUT_PARAMS.debug || false;

    logDebug(`Input parameters: ${JSON.stringify(INPUT_PARAMS)}`, debug);

    // Step 1: Get the RAG service name
    let rag_service_name;
    const service_stmt = snowflake.createStatement({
        sqlText: `
        SELECT fq_rag_service_name
        FROM mygpt.profile.t_service_registry
        WHERE service_id = :1
          AND is_active = true
        LIMIT 1
        `,
        binds: [service_id]
    });
    
    const service_result = service_stmt.execute();
    if (service_result.next()) {
        rag_service_name = service_result.getColumnValue(1);
        logDebug(`Using RAG service: ${rag_service_name}`, debug);
    } else {
        throw new Error(`No active service found for service_id: ${service_id}`);
    }

    // Step 2: Summarize and contextualize the latest questions
    const sortedKeys = Object.keys(latest_prompts).sort((a, b) => Number(a) - Number(b));
    const sortedQuestions = sortedKeys.map(key => latest_prompts[key]);
    
    const summarization_prompt = `Summarize the following questions, focusing on the context relevant to the last question. Emphasize the content of the last question in your summary:\n\n${sortedQuestions.join('\n')}`;
    
    logDebug(`Summarization prompt: ${summarization_prompt}`, debug);

    let question_summary;
    const summary_stmt = snowflake.createStatement({
        sqlText: `
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama2-70b-chat',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT('role', 'system', 'content', 'You are a helpful AI assistant specializing in summarizing questions.'),
                OBJECT_CONSTRUCT('role', 'user', 'content', :1)
            ),
            OBJECT_CONSTRUCT()
        ) as response`,
        binds: [summarization_prompt]
    });
    
    const summary_result = summary_stmt.execute();
    if (summary_result.next()) {
        question_summary = summary_result.getColumnValue(1);
        logDebug(`Question summary: ${question_summary}`, debug);
    } else {
        throw new Error("Failed to generate question summary");
    }

    // Step 3: Perform a RAG search using the summary
    logDebug(`Performing RAG search with service: ${rag_service_name}, query: ${question_summary}`, debug);

    let rag_results;
    const search_stmt = snowflake.createStatement({
        sqlText: `
        SELECT PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                :1,
                OBJECT_CONSTRUCT(
                    'query', :2,
                    'columns', ARRAY_CONSTRUCT('transcript_text', 'region'),
                    'limit', 3
                )
            )
        )['results'] as results`,
        binds: [rag_service_name, question_summary]
    });
    
    const search_result = search_stmt.execute();
    if (search_result.next()) {
        rag_results = search_result.getColumnValue(1);
        logDebug(`RAG search results: ${JSON.stringify(rag_results)}`, debug);
    } else {
        throw new Error("Failed to perform RAG search");
    }

    // Step 4: Generate a final response
    const last_question = latest_prompts[sortedKeys[sortedKeys.length - 1]];
    
    const response_prompt = `
    Context: ${question_summary}
    
    RAG Search Results:
    ${JSON.stringify(rag_results, null, 2)}
    
    User Question: ${last_question}
    
    Please provide a helpful response based on the context, RAG search results, and the user's question.`;
    
    logDebug(`Response generation prompt: ${response_prompt}`, debug);

    let llm_response;
    const response_stmt = snowflake.createStatement({
        sqlText: `
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama2-70b-chat',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT('role', 'system', 'content', 'You are a helpful AI assistant. Use the provided context and search results to answer the user\'s question.'),
                OBJECT_CONSTRUCT('role', 'user', 'content', :1)
            ),
            OBJECT_CONSTRUCT()
        ) as response`,
        binds: [response_prompt]
    });
    
    const response_result = response_stmt.execute();
    if (response_result.next()) {
        llm_response = response_result.getColumnValue(1);
        logDebug(`Generated response: ${llm_response}`, debug);
    } else {
        throw new Error("Failed to generate response");
    }

    // Return the results
    return {
        llm_response: llm_response,
        question_summary: question_summary
    };
} catch (error) {
    // Log any errors that occur during execution
    snowflake.createStatement({
        sqlText: "CALL system$log_error(?)",
        binds: [`Error in rag_search_and_respond: ${error.message}`]
    }).execute();
    throw error;
}
$$;
