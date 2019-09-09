CREATE OR REPLACE FUNCTION move_reserved_loans() RETURNS INTEGER AS
  $body$
    DECLARE
        loan RECORD;
        loan_move_counter INTEGER := 0;
    BEGIN
      FOR loan IN SELECT * FROM book_loanreserved LOOP
        IF NOT EXISTS (SELECT 1 FROM book_loan WHERE book_copy_id = loan.book_copy_id AND to_date > current_date) THEN
            INSERT INTO book_loan (user_id, book_copy_id, from_date, to_date, loan_start_information, loan_end_information) VALUES (loan.user_id, loan.book_copy_id, current_date, current_date+loan.duration, false, false);
            DELETE FROM book_loanreserved WHERE id = loan.id;
            loan_move_counter := loan_move_counter+1;
        END IF;
      END LOOP;
      RETURN loan_move_counter;
    END;
  $body$
LANGUAGE plpgsql;