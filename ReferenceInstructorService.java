package Implementation;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.CourseSection;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.InstructorService;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ReferenceInstructorService implements InstructorService {
    @Override
    public void addInstructor(int userId, String firstName, String lastName) {
        try {/*
            if (userId <= 0 || userId > Integer.MAX_VALUE) {
                throw new IntegrityViolationException();
            }*/
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("insert into user1 (id,firstname,lastname,type) values  (?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
            stmt.setInt(1, userId);
            stmt.setString(2, firstName);
            stmt.setString(3, lastName);
            stmt.setInt(4,2);
            stmt.execute();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IntegrityViolationException();
        }
    }

    @Override
    public List<CourseSection> getInstructedCourseSections(int instructorId, int semesterId) {
        List<CourseSection>  courseSectionList = new ArrayList<>();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
             String sql = "select count(*) from user1 where id = ?  and type = 2";
             PreparedStatement pst = connection.prepareStatement(sql);
             pst.setInt(1,instructorId);
             ResultSet resultSet =  pst.executeQuery();
             int exist = 0;
            if (resultSet.next()){
            exist = resultSet.getInt(1);
        }
        if (exist==0){
            connection.commit();
            connection.close();
            throw new EntityNotFoundException();
        }

            String sql1 = "select count(*) from semester where id = ? ";
            pst = connection.prepareStatement(sql1);
            pst.setInt(1,semesterId);
            ResultSet resultSet1 =  pst.executeQuery();
            int exist1 = 0;
            if (resultSet1.next()){
                exist1 = resultSet1.getInt(1);
            }
            if (exist1==0){
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
             PreparedStatement stmt = connection.prepareStatement
                     ("select CourseSection.* from CourseSection join CourseSectionClass on CourseSectionId=CourseSection.id where instructor=? and semesterId=?");
            stmt.setInt(1, instructorId);
            stmt.setInt(2, semesterId);
            resultSet = stmt.executeQuery();
            while (resultSet.next()){
                int id = resultSet.getInt(1);
                String sectionName = resultSet.getString(4);
                int totalC = resultSet.getInt(5);
                int totalL = resultSet.getInt(6);
                CourseSection courseSection = new CourseSection();
                courseSection.id = id;
                courseSection.name = sectionName;
                courseSection.leftCapacity = totalL;
                courseSection.totalCapacity = totalC;
                courseSectionList.add(courseSection);
            }
            connection.commit();
            connection.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return courseSectionList;
    }
}
