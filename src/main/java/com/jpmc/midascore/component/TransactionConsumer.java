package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.service.IncentiveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final IncentiveService incentiveService;

    @Autowired
    public TransactionConsumer(UserRepository userRepository, 
                               TransactionRecordRepository transactionRecordRepository,
                               IncentiveService incentiveService) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.incentiveService = incentiveService;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    @Transactional
    public void listen(Transaction transaction) {
        logger.info("Processing transaction: {}", transaction);

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null) {
            logger.warn("Discarding transaction: Sender with ID {} does not exist.", transaction.getSenderId());
            return;
        }

        if (recipient == null) {
            logger.warn("Discarding transaction: Recipient with ID {} does not exist.", transaction.getRecipientId());
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Discarding transaction: Sender {} has insufficient balance ({} < {}).",
                    sender.getName(), sender.getBalance(), transaction.getAmount());
            return;
        }

        // Call the incentive API to get the incentive amount
        float incentiveAmount = incentiveService.getIncentive(transaction);

        // Update balances: sender loses ONLY transaction amount, recipient gains transaction amount + incentive
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        // Update database records
        userRepository.save(sender);
        userRepository.save(recipient);

        // Save transaction record with the computed incentive amount
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        transactionRecordRepository.save(record);

        logger.info("Successfully completed transaction from {} (new balance: {}) to {} (new balance: {}) for amount {} with incentive {}",
                sender.getName(), sender.getBalance(), recipient.getName(), recipient.getBalance(), transaction.getAmount(), incentiveAmount);
    }
}
