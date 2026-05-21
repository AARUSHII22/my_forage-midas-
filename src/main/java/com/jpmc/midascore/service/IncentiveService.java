package com.jpmc.midascore.service;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class IncentiveService {
    private final RestTemplate restTemplate;
    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";

    @Autowired
    public IncentiveService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public float getIncentive(Transaction transaction) {
        try {
            Incentive response = restTemplate.postForObject(INCENTIVE_API_URL, transaction, Incentive.class);
            if (response != null) {
                return response.getAmount();
            }
        } catch (Exception e) {
            // Fallback to 0.0f if the API is down or returns an error
        }
        return 0.0f;
    }
}
