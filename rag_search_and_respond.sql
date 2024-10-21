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
        ORDER BY effective_date DESC
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
            ORDER BY effective_date DESC
            LIMIT 1
            `
        });
        
        const default_result = default_query.execute();
        
        if (default_result.next()) {
            return default_result.getColumnValue(1);
        } else {
            throw new Error(`No service found for domain '${domain}' and no default service available`);
        }
    }
}

/**
 * Summarizes a list of questions using Snowflake Cortex LLM.
 * @param {Object} questions - The object containing questions as key-value pairs.
 * @returns {string} A summary of the questions.
 * @throws {Error} If the summarization fails.
 */
function summarize_questions(questions) {
    // Sort the questions by their keys (assuming keys are sortable, like timestamps or sequence numbers)
    const sortedKeys = Object.keys(questions).sort();
    const sortedQuestions = sortedKeys.map(key => questions[key]);
    
    const summarization_prompt = `Summarize the following questions, focusing on the context relevant to the last question. Emphasize the content of the last question in your summary:\n\n${sortedQuestions.join('\n')}`;
    
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
 * @param {string} service_name - The name of the search service to use.
 * @param {string} query - The search query.
 * @returns {Object} The search results.
 * @throws {Error} If the RAG search fails.
 */
function perform_rag_search(service_name, query) {
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
        binds: [service_name, query]
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
 * @returns {string} The generated response.
 * @throws {Error} If the response generation fails.
 */
function generate_response(rag_results, last_question, context) {
    const response_prompt = `
    Context: ${context}
    
    RAG Search Results:
    ${JSON.stringify(rag_results, null, 2)}
    
    User Question: ${last_question}
    
    Please provide a helpful response based on the context, RAG search results, and the user's question.`;
    
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
    validateInput(input_params);

    const domain_name = input_params.domain_name;
    const latest_prompts = input_params.latest_prompts;

    // Step 1: Determine the correct service name
    const service_name = get_service_name(domain_name);
    snowflake.log('info', `Using service: ${service_name}`);

    // Step 2: Summarize and contextualize the latest questions
    const question_summary = summarize_questions(latest_prompts);
    snowflake.log('info', `Question summary: ${question_summary}`);

    // Step 3: Perform a RAG search using the summary
    const rag_results = perform_rag_search(service_name, question_summary);
    snowflake.log('info', `RAG search results: ${JSON.stringify(rag_results)}`);

    // Step 4: Generate a final response
    const sortedKeys = Object.keys(latest_prompts).sort();
    const last_question = latest_prompts[sortedKeys[sortedKeys.length - 1]];
    const llm_response = generate_response(rag_results, last_question, question_summary);
    snowflake.log('info', `Generated response: ${llm_response}`);

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
