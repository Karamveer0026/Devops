import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PersonStore {
    private Connection connection;

    public PersonStore(Connection connection) {
        this.connection = connection;
    }

    public void addPerson(Person p) throws PersonAlreadyExistsException {
        String query = "INSERT INTO person (person_id, person_name, location, date_of_birth) VALUES (?, ?, ?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, p.getPersonId());
            stmt.setString(2, p.getPersonName());
            stmt.setString(3, p.getLocation());
            stmt.setDate(4, Date.valueOf(p.getDateOfBirth()));
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new PersonAlreadyExistsException("Person already exists in the database.");
        }
    }

    public void updatePerson(Person p) {
        String query = "UPDATE person SET person_name = ?, location = ?, date_of_birth = ? WHERE person_id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, p.getPersonName());
            stmt.setString(2, p.getLocation());
            stmt.setDate(3, Date.valueOf(p.getDateOfBirth()));
            stmt.setInt(4, p.getPersonId());
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<Person> getAllPersons() {
        List<Person> persons = new ArrayList<>();
        String query = "SELECT * FROM person";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                Person p = new Person();
                p.setPersonId(rs.getInt("person_id"));
                p.setPersonName(rs.getString("person_name"));
                p.setLocation(rs.getString("location"));
                p.setDateOfBirth(rs.getDate("date_of_birth").toLocalDate());
                persons.add(p);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return persons;
    }

    public List<Person> getPersonByCity(String location) {
        List<Person> persons = new ArrayList<>();
        String query = "SELECT * FROM person WHERE location = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, location);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Person p = new Person();
                    p.setPersonId(rs.getInt("person_id"));
                    p.setPersonName(rs.getString("person_name"));
                    p.setLocation(rs.getString("location"));
                    p.setDateOfBirth(rs.getDate("date_of_birth").toLocalDate());
                    persons.add(p);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return persons;
    }

    public void addTeam(Person p1, Person p2) throws PersonNotAddedException {
        String query = "INSERT INTO person (person_id, person_name, location, date_of_birth) VALUES (?, ?, ?, ?)";
        try {
            connection.setAutoCommit(false);

            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.setInt(1, p1.getPersonId());
                stmt.setString(2, p1.getPersonName());
                stmt.setString(3, p1.getLocation());
                stmt.setDate(4, Date.valueOf(p1.getDateOfBirth()));
                stmt.executeUpdate();

                stmt.setInt(1, p2.getPersonId());
                stmt.setString(2, p2.getPersonName());
                stmt.setString(3, p2.getLocation());
                stmt.setDate(4, Date.valueOf(p2.getDateOfBirth()));
                stmt.executeUpdate();

                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw new PersonNotAddedException("Failed to add team members.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new PersonNotAddedException("Failed to add team members.");
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
