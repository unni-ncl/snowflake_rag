CREATE OR REPLACE FUNCTION rag_search_and_respond(INPUT_PARAMS OBJECT)
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

    // Validate service_id
    if (!params.hasOwnProperty('service_id')) {
        throw new Error("Missing required parameter: service_id");
    }
    if (typeof params.service_id !== 'string' || params.service_id.trim() === '') {
        throw new Error("service_id must be a non-empty string");
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
 * Retrieves the fully qualified RAG service name for a given service ID.
 * @param {string} service_id - The service ID to look up.
 * @returns {string} The fully qualified RAG service name.
 * @throws {Error} If no active service is found.
 */
/**
 * Retrieves the fully qualified RAG service name for a given service ID.
 * @param {number} service_id - The service ID to look up.
 * @returns {string} The fully qualified RAG service name.
 * @throws {Error} If no active service is found.
 */
function get_rag_service_name(service_id) {
    const result = snowflake.execute({
        sqlText: `
        SELECT fq_rag_service_name
        FROM mygpt.profile.t_service_registry
        WHERE service_id = :1
          AND is_active = true
        LIMIT 1
        `,
        binds: [service_id]
    });
    
    if (result.next()) {
        return result.getColumnValue(1);
    } else {
        throw new Error(`No active service found for service_id: ${service_id}`);
    }
}

/**
 * Summarizes a list of questions using Snowflake Cortex LLM.
 * @param {Object} questions - The object containing questions with epoch timestamps.
 * @param {boolean} debug - Flag indicating whether to enable debug logging.
 * @returns {string} A summary of the questions.
 * @throws {Error} If the summarization fails.
 */
function summarize_questions(questions, debug) {
    // Sort the questions by epoch timestamp
    const sortedKeys = Object.keys(questions).sort((a, b) => Number(a) - Number(b));
    const sortedQuestions = sortedKeys.map(key => questions[key]);
    
    const summarization_prompt = `Summarize the following questions, focusing on the context relevant to the last question. Emphasize the content of the last question in your summary:\n\n${sortedQuestions.join('\n')}`;
    
    if (debug) {
        snowflake.log('info', `Summarization prompt: ${summarization_prompt}`);
    }

    const summary = snowflake.execute({
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
    
    if (summary.next()) {
        return summary.getColumnValue(1);
    } else {
        throw new Error("Failed to generate question summary");
    }
}

/**
 * Performs a RAG search using the provided service name and query.
 * @param {string} rag_service_name - The fully qualified name of the search service.
 * @param {string} query - The search query.
 * @param {boolean} debug - Flag indicating whether to enable debug logging.
 * @returns {Object} The search results.
 * @throws {Error} If the RAG search fails.
 */
function perform_rag_search(rag_service_name, query, debug) {
    if (debug) {
        snowflake.log('info', `Performing RAG search with service: ${rag_service_name}`);
        snowflake.log('info', `Search query: ${query}`);
    }

    const search_results = snowflake.execute({
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
        binds: [rag_service_name, query]
    });
    
    if (search_results.next()) {
        return search_results.getColumnValue(1);
    } else {
        throw new Error("Failed to perform RAG search");
    }
}

/**
 * Generates a response using Snowflake Cortex LLM based on RAG results and user question.
 * @param {Object} rag_results - The results from the RAG search.
 * @param {string} last_question - The user's last question.
 * @param {string} context - The context derived from summarizing previous questions.
 * @param {boolean} debug - Flag indicating whether to enable debug logging.
 * @returns {string} The generated response.
 * @throws {Error} If the response generation fails.
 */
function generate_response(rag_results, last_question, context, debug) {
    const response_prompt = `
    Context: ${context}
    
    RAG Search Results:
    ${JSON.stringify(rag_results, null, 2)}
    
    User Question: ${last_question}
    
    Please provide a helpful response based on the context, RAG search results, and the user's question.`;
    
    if (debug) {
        snowflake.log('info', `Response generation prompt: ${response_prompt}`);
    }

    const response = snowflake.execute({
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
    
    if (response.next()) {
        return response.getColumnValue(1);
    } else {
        throw new Error("Failed to generate response");
    }
}

// Main function execution
try {
    // Input validation
    validateInput(INPUT_PARAMS);

    const service_id = INPUT_PARAMS.service_id;
    const latest_prompts = INPUT_PARAMS.latest_prompts;
    const debug = INPUT_PARAMS.debug || false;

    // Log input parameters if debug mode is enabled
    if (debug) {
        snowflake.log('info', `Input parameters: ${JSON.stringify(INPUT_PARAMS)}`);
    }

    // Step 1: Get the RAG service name
    const rag_service_name = get_rag_service_name(service_id);
    if (debug) {
        snowflake.log('info', `Using RAG service: ${rag_service_name}`);
    }

    // Step 2: Summarize and contextualize the latest questions
    const question_summary = summarize_questions(latest_prompts, debug);
    if (debug) {
        snowflake.log('info', `Question summary: ${question_summary}`);
    }

    // Step 3: Perform a RAG search using the summary
    const rag_results = perform_rag_search(rag_service_name, question_summary, debug);
    if (debug) {
        snowflake.log('info', `RAG search results: ${JSON.stringify(rag_results)}`);
    }

    // Step 4: Generate a final response
    const sortedKeys = Object.keys(latest_prompts).sort((a, b) => Number(a) - Number(b));
    const last_question = latest_prompts[sortedKeys[sortedKeys.length - 1]];
    const llm_response = generate_response(rag_results, last_question, question_summary, debug);
    if (debug) {
        snowflake.log('info', `Generated response: ${llm_response}`);
    }

    // Return the results
    return {
        llm_response: llm_response,
        question_summary: question_summary
    };
} catch (error) {
    // Log any errors that occur during execution
    snowflake.log('error', `Error in rag_search_and_respond: ${error.message}`);
    throw error;
}
$$;
