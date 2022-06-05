delete from assessment_map_v1 
where school_assessment_instance_id > 44;

delete from assessment_star_v1
where school_assessment_instance_id > 44;

delete from student_assessment
where school_assessment_instance_id > 44;

delete from school_assessment_instance
where id > 44;

delete from student_opportunity where student_id > 1;
delete from student_support where student_id > 1;
delete from student_obstacle where student_id > 1;
delete from student where id > 1;
