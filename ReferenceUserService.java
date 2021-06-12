package Implementation;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.service.UserService;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ReferenceUserService implements UserService {
    @Override
    public void removeUser(int userId) {
        try {
            int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "SELECT user1.type from user1 where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1, userId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist > 0) {
                if (exist == 2) {
                    //instructor
                    pst = connection.prepareStatement("DELETE FROM courseSectionClass where instructor = ?");
                    pst.setInt(1, userId);
                    pst.executeUpdate();
                    pst = connection.prepareStatement("DELETE FROM user1 where id = ?");
                    pst.setInt(1, userId);
                    pst.executeUpdate();
                } else if (exist == 1) {
                    pst = connection.prepareStatement("DELETE FROM studentGrades where studentid = ?");
                    pst.setInt(1, userId);
                    pst.executeUpdate();
                    pst = connection.prepareStatement("DELETE FROM user1 where id = ?");
                    pst.setInt(1, userId);
                    pst.executeUpdate();
                }
                connection.commit();
                connection.close();
            } else {
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<User> getAllUsers() {
        List<User> ulist = new ArrayList<>();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select * from user1 ";
            PreparedStatement pst = connection.prepareStatement(sql);
            ResultSet resultSet = pst.executeQuery();

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String firstname = resultSet.getString(2);
                String lastname = resultSet.getString(3);
                Date enrolledDate = resultSet.getDate(4);
                int majorId = resultSet.getInt(5);
                int type = resultSet.getInt(6);
                if (type == 2) {
                    //instructor
                    Instructor instructor = new Instructor();
                    instructor.id = id;
                    String fullname;
                    if (firstname.matches("^[A-Za-z]+$") && lastname.matches("^[A-Za-z]+$"))
                        fullname = firstname + " " + lastname;
                    else
                        fullname = firstname + lastname;
                    instructor.fullName = fullname;
                    ulist.add(instructor);
                } else if (type == 1) {
                    //student
                    Student student = new Student();
                    pst = connection.prepareStatement("select major.name,department.id,department.name from major join department on major.departmentId = department.id where major.id = ?");
                    pst.setInt(1, majorId);
                    ResultSet res = pst.executeQuery();
                    if (res.next()) {
                        String majorName = res.getString(1);
                        int departmentId = res.getInt(2);
                        String departmentName = res.getString(3);
                        student.id = id;
                        String fullname;
                        if (firstname.matches("^[A-Za-z]+$") && lastname.matches("^[A-Za-z]+$"))
                            fullname = firstname + " " + lastname;
                        else
                            fullname = firstname + lastname;
                        student.fullName = fullname;
                        student.enrolledDate = enrolledDate;
                        student.major = new Major();
                        student.major.id = majorId;
                        student.major.name = majorName;
                        student.major.department = new Department();
                        student.major.department.id = departmentId;
                        student.major.department.name = departmentName;
                        ulist.add(student);
                    }
                }
            }
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ulist;
    }

    @Override
    public User getUser(int userId) {
        User user = null;
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select * from user1 where user1.id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,userId);
            ResultSet resultSet =  pst.executeQuery();
            if(resultSet.next()){
                int id=resultSet.getInt(1);
                String firstname =resultSet.getString(2);
                String lastname =resultSet.getString(3);
                Date enrolledDate = resultSet.getDate(4);
                int majorId = resultSet.getInt(5);
                int type = resultSet.getInt(6);
                if (type==2){
                    //instructor
                    Instructor instructor = new Instructor();
                    instructor.id = id;
                    String fullname;
                    if(firstname.matches("^[A-Za-z]+$") && lastname.matches("^[A-Za-z]+$"))
                        fullname = firstname + " " + lastname;
                    else
                        fullname = firstname + lastname;
                    instructor.fullName = fullname;
                    user = instructor;
                }else if (type==1){
                    //student
                    Student student = new Student();
                    pst = connection.prepareStatement("select major.name,department.id,department.name from major join department on major.departmentId = department.id where major.id = ?");
                    pst.setInt(1,majorId);
                    ResultSet res = pst.executeQuery();
                    String majorName = res.getString(1);
                    int departmentId = res.getInt(2);
                    String departmentName = res.getString(3);
                    student.id = id;
                    String fullname;
                    if(firstname.matches("^[A-Za-z]+$") && lastname.matches("^[A-Za-z]+$"))
                        fullname = firstname + " " + lastname;
                    else
                        fullname = firstname + lastname;
                    student.fullName = fullname;
                    student.enrolledDate = enrolledDate;
                    student.major = new Major();
                    student.major.id = majorId;
                    student.major.name = majorName;
                    student.major.department = new Department();
                    student.major.department.id = departmentId;
                    student.major.department.name = departmentName;
                    user = student;
                }
            }else {
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
            connection.commit();
            connection.close();
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return user;
    }
}
