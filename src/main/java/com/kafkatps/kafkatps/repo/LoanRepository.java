package com.kafkatps.kafkatps.repo;




import com.kafkatps.kafkatps.enti.Loan;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.repository.CrudRepository;

import java.util.UUID;
public interface LoanRepository extends R2dbcRepository<Loan, UUID> {
}
