package com.postgresbank.phase3_coordination;

import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentJobRepository extends JpaRepository<PaymentJob, Long> {

  long countByStatus(String status);
}
