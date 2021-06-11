package Implementation;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.CourseSection;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.InstructorService;

import java.sql.*;
import java.util.List;

public class ReferenceInstructorService implements InstructorService {
    @Override
    public void addInstructor(int userId, String firstName, String lastName) {
        try {
            if (userId <= 0 || userId > Integer.MAX_VALUE) {
                throw new IntegrityViolationException();
            }
            int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            String sql="SELECT count(*) from instructor where id = ? and firstname=? and lastname = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,userId);
            pst.setString(2,firstName);
            pst.setString(3,lastName);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist>0) {
                connection.close();
                throw new IntegrityViolationException();
            }

            sql="SELECT count(*) from user1 where id = ? and firstname=? and lastname = ?;";
            pst = connection.prepareStatement(sql);
            pst.setInt(1,userId);
            pst.setString(2,firstName);
            pst.setString(3,lastName);
            resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist>0) {
                connection.close();
                throw new IntegrityViolationException();
            }
            Integer enterInfoId = null;
            //connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("insert into Instructor(id,firstname,lastname) values (?,?,?);");
            stmt.setInt(1, userId);
            stmt.setString(2, firstName);
            stmt.setString(3, lastName);
            stmt.execute();
            stmt = connection.prepareStatement("insert into user1 (id,firstname, lastname,role) values  (?,?,?,?);", Statement.RETURN_GENERATED_KEYS);
            stmt.setInt(1, userId);
            stmt.setString(2, firstName);
            stmt.setString(3, lastName);
            stmt.setInt(4,2);

            stmt.execute();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    @Override
    public List<CourseSection> getInstructedCourseSections(int instructorId, int semesterId) {
        ResultSet rst = null;
        try {Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
             String sql = "select count(*) from instructor where id = ? ";
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
            PreparedStatement pst1 = connection.prepareStatement(sql1);
            pst1.setInt(1,semesterId);
            ResultSet resultSet1 =  pst1.executeQuery();
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
                     ("select CourseSection.* from CourseSection join CourseSectionClass on CourseSectionId=CourseSection.id\n" +
                     "    where instructor=? and semesterId=?;");
            stmt.setInt(1, instructorId);
            stmt.setInt(2, semesterId);

            stmt.executeUpdate();
            connection.commit();
            connection.close();
        }
        //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
        catch (SQLException e) {
            e.printStackTrace();
        }
        return (List) rst;
    }
}
